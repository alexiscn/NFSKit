//
//  NFSContext.swift
//  NFSKit
//
//  Created by alexiscn on 2021/7/17.
//

import nfs
import Foundation

class NFSContext {
    
    var context: UnsafeMutablePointer<nfs_context>?
    
    private var contextLock = NSRecursiveLock()
    
    var timeout: TimeInterval
    
    init(timeout: TimeInterval) throws {
        let _context = try nfs_init_context().unwrap()
        self.context = _context
        self.timeout = timeout
    }
    
    deinit {
        if isConnected {
            try? disconnect()
        }
        try? withThreadSafeContext({ context in
            self.context = nil
            nfs_destroy_context(context)
        })
    }
}

// MARK: Connectivity
extension NFSContext {
    
    func getexports(server: String) throws -> [String] {
        return try async_await(dataHandler: { context, data in
            let result = try data.unwrap().assumingMemoryBound(to: exports.self).pointee
            var export: exportnode? = result.pointee
            var list: [String] = []
            while export != nil {
                if let dir = export?.ex_dir {
                    list.append(String(cString: dir))
                }
                export = export?.ex_next?.pointee
            }
            return list
        }, execute: { context, cbPtr in
            let rpc = nfs_get_rpc_context(context)
            return mount_getexports_async(rpc, server, NFSContext.rpc_handler, cbPtr)
        }).data
    }
 
    func connect(server: String, export: String) throws {
        try async_await(execute: { context, cbPtr in
            nfs_mount_async(context, server, export, NFSContext.generic_handler, cbPtr)
        })
    }
    
    func disconnect() throws {
        try async_await(execute: { context, cbPtr in
            nfs_umount_async(context, NFSContext.generic_handler, cbPtr)
        })
    }
    
    func autoguid() throws -> (Int32, Int32) {
        let stat = try stat("/")
        let context = try context.unwrap()
        let gid = Int32(stat.nfs_gid)
        let uid = Int32(stat.nfs_uid)
        nfs_set_uid(context, uid)
        nfs_set_gid(context, gid)
        return (uid, gid)
    }
}

// MARK: - File Information
extension NFSContext {
    
    func stat(_ path: String) throws -> nfs_stat_64 {
        return try async_await(dataHandler: Parser.tostat64, execute: { context, cbPtr in
            nfs_stat64_async(context, path, NFSContext.generic_handler, cbPtr)
        }).data
    }
    
    func statvfs(_ path: String) throws -> statvfs {
        return try async_await(dataHandler: Parser.tostatvfs, execute: { context, cbPtr in
            nfs_statvfs_async(context, path, NFSContext.generic_handler, cbPtr)
        }).data
    }
    
    func readlink(_ path: String) throws -> String {
        return try async_await(dataHandler: Parser.toString) { (context, cbPtr) -> Int32 in
            nfs_readlink_async(context, path, NFSContext.generic_handler, cbPtr)
        }.data
    }
}

extension NFSContext {
    
    var isConnected: Bool {
        do {
            return try withThreadSafeContext { (context) -> Bool in
                context.pointee.server != nil && context.pointee.rpc.pointee.is_connected != 0
            }
        } catch {
            return false
        }
    }
    
    var server: String? {
        if let server = context?.pointee.server {
            return String(cString: server)
        }
        return nil
    }
    
    var export: String? {
        if let export = context?.pointee.export {
            return String(cString: export)
        }
        return nil
    }
    
    var fileDescriptor: Int32 {
        return (try? nfs_get_fd(context.unwrap())) ?? -1
    }
    
    var error: String? {
        if let errorStr = nfs_get_error(context) {
            return String(cString: errorStr)
        }
        return nil
    }
    
    func whichEvents() throws -> Int16 {
        return try Int16(truncatingIfNeeded: nfs_which_events(context.unwrap()))
    }
    
    func service(revents: Int32) throws {
        try withThreadSafeContext { context in
            let result = nfs_service(context, revents)
            if result < 0 {
                self.context = nil
                nfs_destroy_context(context)
            }
            try POSIXError.throwIfError(result, description: error)
        }
    }
    
}

// MARK: - File Operation
extension NFSContext {
    
    func mkdir(_ path: String) throws {
        try async_await(execute: { context, cbPtr in
            nfs_mkdir_async(context, path, NFSContext.generic_handler, cbPtr)
        })
    }
    
    func rmdir(_ path: String) throws {
        try async_await(execute: { context, cbPtr in
            nfs_rmdir_async(context, path, NFSContext.generic_handler, cbPtr)
        })
    }
    
    func unlink(_ path: String) throws {
        try async_await { (context, cbPtr) -> Int32 in
            nfs_unlink_async(context, path, NFSContext.generic_handler, cbPtr)
        }
    }
    
    func rename(_ path: String, to newPath: String) throws {
        try async_await(execute: { context, cbPtr in
            nfs_rename_async(context, path, newPath, NFSContext.generic_handler, cbPtr)
        })
    }
    
    func truncate(_ path: String, toLength: UInt64) throws {
        try async_await { (context, cbPtr) -> Int32 in
            nfs_truncate_async(context, path, toLength, NFSContext.generic_handler, cbPtr)
        }
    }
}



// MARK: Async operation handler
extension NFSContext {
    
    private class CBData {
        var result: Int32 = 0
        var isFinished: Bool = false
        var dataHandler: ((Int32, UnsafeMutableRawPointer?) -> Void)? = nil
        var status: UInt32 {
            return UInt32(bitPattern: result)
        }
    }
    
    private func wait_for_reply(_ cb: inout CBData) throws {
        let startDate = Date()
        while !cb.isFinished {
            var pfd = pollfd()
            pfd.fd = fileDescriptor
            pfd.events = try whichEvents()
            
            if pfd.fd < 0 || (poll(&pfd, 1, 1000) < 0 && errno != EAGAIN) {
                throw POSIXError(.init(errno), description: error)
            }
            
            if pfd.revents == 0 {
                if timeout > 0, Date().timeIntervalSince(startDate) > timeout {
                    throw POSIXError(.ETIMEDOUT)
                }
                continue
            }
            
            try service(revents: Int32(pfd.revents))
        }
    }
    
    static let generic_handler: nfs_cb = { error, nfs, data, cbdata in
        do {
            let cbdata = try cbdata.unwrap().bindMemory(to: CBData.self, capacity: 1).pointee
            if error != 0 {
                cbdata.result = error
            }
            cbdata.dataHandler?(error, data)
            cbdata.isFinished = true
        } catch { }
    }
    
    static let rpc_handler: rpc_cb = { rpc, status, data, cbdata in
        do {
            let cbdata = try cbdata.unwrap().bindMemory(to: CBData.self, capacity: 1).pointee
            if status != 0 {
                cbdata.result = status
            }
            cbdata.dataHandler?(status, data)
            cbdata.isFinished = true
        } catch { }
    }
    
    func withThreadSafeContext<R>(_ handler: (UnsafeMutablePointer<nfs_context>) throws -> R) throws -> R {
        contextLock.lock()
        defer {
            contextLock.unlock()
        }
        return try handler(context.unwrap())
    }
    
    typealias ContextHandler<R> = (_ context: UnsafeMutablePointer<nfs_context>, _ dataPtr: UnsafeMutableRawPointer?) throws -> R
        
    @discardableResult
    func async_await(execute handler: ContextHandler<Int32>) throws -> Int32 {
        return try async_await(dataHandler: { _, _ in }, execute: handler).result
    }
    
    @discardableResult
    func async_await<DataType>(dataHandler: @escaping ContextHandler<DataType>, execute handler: ContextHandler<Int32>)
            throws -> (result: Int32, data: DataType) {
        return try withThreadSafeContext { (context) -> (Int32, DataType) in
            var cb = CBData()
            var resultData: DataType?
            var dataHandlerError: Error?
            cb.dataHandler = { status, ptr in
                do {
                    try POSIXError.throwIfError(status, description: self.error)
                    resultData = try dataHandler(context, ptr)
                } catch {
                    dataHandlerError = error
                }
            }
            let result = try handler(context, &cb)
            try POSIXError.throwIfError(result, description: error)
            try wait_for_reply(&cb)
            let cbResult = cb.result
            
            try POSIXError.throwIfError(cbResult, description: error)
            if let error = dataHandlerError { throw error }
            return try (cbResult, resultData.unwrap())
        }
    }
}
