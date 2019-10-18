//
//  NFSContext.swift
//  NetworkFileSystemKit
//
//  Created by xu.shuifeng on 2019/10/17.
//  Copyright © 2019 alexiscn. All rights reserved.
//

import Foundation
import nfs

final class NFSContext  {
    
    var context: UnsafeMutablePointer<nfs_context>?
    
    var timeout: TimeInterval
    
    private var _context_lock = NSLock()
    
    var version: Int32 { return self.context?.pointee.version ?? -1 }
    
    var nfsPort: Int32 { return self.context?.pointee.nfsport ?? -1 }
    
    var mountPort: Int32 { return self.context?.pointee.mountport ?? -1 }
    
    var error: String? {
        if let errorString = nfs_get_error(context) {
            return String(cString: errorString)
        }
        return nil
    }
    
    var fileDescriptor: Int32 { return nfs_get_fd(context) }
    
    static let generic_handler: nfs_cb = async_handler()
    
    init(timeout: TimeInterval = 5) throws {
        guard let _context = nfs_init_context() else {
            throw POSIXError(.ENOMEM)
        }
        self.context = _context
        self.timeout = timeout
    }
    
    deinit {
        try? withThreadSafeContext { context in
            nfs_destroy_context(context)
        }
    }
    
    func withThreadSafeContext<R>(_ handler: (UnsafeMutablePointer<nfs_context>) throws -> R) throws -> R {
        guard let context = self.context else {
            throw POSIXError(POSIXErrorCode.ECONNABORTED)
        }
        _context_lock.lock()
        defer {
            _context_lock.unlock()
        }
        return try handler(context)
    }
    
    static func async_handler() -> nfs_cb {
        return { err, nfs, data, cbdata in
            guard let cbdata = cbdata?.bindMemory(to: CBData.self, capacity: 1).pointee else { return }
            if err < 0 {
                cbdata.result = err
            }
            cbdata.data = data
            cbdata.isFinish = true
        }
    }
    
    func whichEvents() -> Int32 {
        return nfs_which_events(context)
    }
    
    func service(revents: Int32) throws {
        let result = try withThreadSafeContext { context in
            return nfs_service(context, revents)
        }
        if result < 0 {
            self._context_lock.lock()
            nfs_destroy_context(context)
            context = nil
            self._context_lock.unlock()
        }
    }
    
    typealias AsyncAwaitHandler<R> = (_ context: UnsafeMutablePointer<nfs_context>, _ cbPtr: UnsafeMutableRawPointer) -> R
    
    @discardableResult
    func async_await(execute handler: AsyncAwaitHandler<Int32>) throws -> (result: Int32, data: UnsafeMutableRawPointer?) {
        var cb = CBData()
        let result = try withThreadSafeContext { (context) -> Int32 in
            return handler(context, &cb)
        }
        try POSIXError.throwIfError(result, description: error, default: .ECONNRESET)
        try wait_for_reply(&cb)
        let cbResult = cb.result
        try POSIXError.throwIfError(cbResult, description: error, default: .ECONNREFUSED)
        let data = cb.data
        return (cbResult, data)
    }
}


// MARK: - mount
extension NFSContext {
    
    func mount(server: String, exportname: String) throws {
        try async_await(execute: { (context, cbPtr) -> Int32 in
            nfs_mount_async(context, server, exportname, NFSContext.generic_handler, cbPtr)
        })
    }
    
    func unmount() throws {
        try async_await(execute: { (context, cbPtr) -> Int32 in
            nfs_umount_async(context, NFSContext.generic_handler, cbPtr)
        })
    }
    
}

extension NFSContext {
    
    private class CBData {
        var result: Int32 = 0
        var data: UnsafeMutableRawPointer? = nil
        var isFinish = false
    }
    
    private func wait_for_reply(_ cb: inout CBData) throws {
        
        let startDate = Date()
        while !cb.isFinish {
            var pfd = pollfd()
            pfd.fd = nfs_get_fd(context)
            pfd.events = Int16(whichEvents())
            
            if poll(&pfd, 1, 1000) < 0, errno != EAGAIN {
                let code = POSIXErrorCode(rawValue: errno) ?? .EINVAL
                throw POSIXError(code, description: error)
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
}


extension POSIXError {
    static func throwIfError(_ result: Int32, description: String?, default: POSIXError.Code) throws {
        guard result < 0 else {
            return
        }
        let errno = -result
        let code = POSIXErrorCode(rawValue: errno) ?? `default`
        let errorDesc = description.map { "Error code \(errno): \($0)" }
        throw POSIXError(code, description: errorDesc)
    }
    
    init(_ code: POSIXError.Code, description: String?) {
        let userInfo: [String: Any] = description.map({ [NSLocalizedFailureReasonErrorKey: $0] }) ?? [:]
        self = POSIXError(code, userInfo: userInfo)
    }
}
