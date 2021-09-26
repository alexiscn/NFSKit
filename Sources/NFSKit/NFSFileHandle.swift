//
//  NFSFileHandle.swift
//  NFSKit
//
//  Created by alexiscn on 2021/7/18.
//

import Foundation
import nfs

class NFSFileHandle {
    
    struct SeekWhence: RawRepresentable {
        var rawValue: Int32
        
        static let set     = SeekWhence(rawValue: SEEK_SET)
        static let current = SeekWhence(rawValue: SEEK_CUR)
        static let end     = SeekWhence(rawValue: SEEK_END)
    }
    
    private var context: NFSContext
    private var handle: UnsafeMutablePointer<nfsfh>?
    
    convenience init(forReadingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_RDONLY, on: context)
    }
    
    convenience init(forWritingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_WRONLY, on: context)
    }
    
    convenience init(forCreatingAndWritingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_WRONLY | O_CREAT | O_TRUNC, on: context)
    }
    
    convenience init(forCreatingIfNotExistsAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_RDWR | O_CREAT | O_EXCL, on: context)
    }
    
    convenience init(forUpdatingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_RDWR | O_APPEND, on: context)
    }
    
    private init(_ path: String, flags: Int32, on context: NFSContext) throws {
        let (_, handle) = try context.async_await(dataHandler: Parser.toOpaquePointer) { (context, cbPtr) -> Int32 in
            nfs_open_async(context, path, flags, NFSContext.generic_handler, cbPtr)
        }
        self.context = context
        self.handle = UnsafeMutablePointer<nfsfh>(handle)
    }
    
    deinit {
        do {
            let handle = try self.handle.unwrap()
            try context.async_await { (context, cbPtr) -> Int32 in
                nfs_close_async(context, handle, NFSContext.generic_handler, cbPtr)
            }
        } catch { }
    }
    
    var fileId: nfs_fh? {
        return try? nfs_get_fh(handle).unwrap().pointee
    }
    
    func close() {
        guard let handle = handle else { return }
        self.handle = nil
        _ = try? context.withThreadSafeContext { (context) in
            nfs_close(context, handle)
        }
    }
    
    func fstat() throws -> nfs_stat_64 {
        let handle = try self.handle.unwrap()
        return try context.async_await { context, data in
            let result = try data.unwrap().assumingMemoryBound(to: nfs_stat_64.self).pointee
            return result
        } execute: { context, cbPtr in
            nfs_fstat64_async(context, handle, NFSContext.generic_handler, cbPtr)
        }.data

    }
    
    func ftruncate(toLength: UInt64) throws {
        let handle = try self.handle.unwrap()
        try context.async_await { (context, cbPtr) -> Int32 in
            nfs_ftruncate_async(context, handle, toLength, NFSContext.generic_handler, cbPtr)
        }
    }
    
    var maxReadSize: Int {
        return (try? Int(context.withThreadSafeContext(nfs_get_readmax))) ?? -1
    }
    
    /// This value allows softer streaming
    var optimizedReadSize: Int {
        return min(maxReadSize, 1048576)
    }
    
    @discardableResult
    func lseek(offset: Int64, whence: SeekWhence) throws -> Int64 {
        let handle = try self.handle.unwrap()
        let result = nfs_lseek(context.context, handle, offset, whence.rawValue, nil)
        if result < 0 {
            try POSIXError.throwIfError(Int32(result), description: context.error)
        }
        return Int64(handle.pointee.offset)
    }
    
    func read(length: Int = 0) throws -> Data {
        precondition(length <= UInt32.max, "Length bigger than UInt32.max can't be handled by libnfs.")
        
        let handle = try self.handle.unwrap()
        let count = length > 0 ? length : optimizedReadSize
        return try context.async_await(dataHandler: { context, data in
            return try Data(bytes: data.unwrap(), count: count)
        }, execute: { (context, cbPtr) -> Int32 in
            nfs_read_async(context, handle, UInt64(count), NFSContext.generic_handler, cbPtr)
        }).data
    }
    
    func pread(offset: UInt64, length: Int = 0) throws -> Data {
        precondition(length <= UInt32.max, "Length bigger than UInt32.max can't be handled by libnfs.")
        
        let handle = try self.handle.unwrap()
        let count = length > 0 ? length : optimizedReadSize
        return try context.async_await(dataHandler: { context, data in
            return try Data(bytes: data.unwrap(), count: count)
        }, execute: { context, cbPtr in
            nfs_pread_async(context, handle, offset, UInt64(count), NFSContext.generic_handler, cbPtr)
        }).data
    }
    
    var maxWriteSize: Int {
        return (try? Int(context.withThreadSafeContext(nfs_get_writemax))) ?? -1
    }
    
    var optimizedWriteSize: Int {
        return min(maxWriteSize, 1048576)
    }
    
    func write<DataType: DataProtocol>(data: DataType) throws -> Int {
        precondition(data.count <= Int32.max, "Data bigger than Int32.max can't be handled by libnfs.")
        
        let handle = try self.handle.unwrap()
        var buffer = Array(data)
        let result = try context.async_await { (context, cbPtr) -> Int32 in
            nfs_write_async(context, handle, UInt64(buffer.count), &buffer, NFSContext.generic_handler, cbPtr)
        }
        return Int(result)
    }
    
    func pwrite<DataType: DataProtocol>(data: DataType, offset: UInt64) throws -> Int {
        precondition(data.count <= Int32.max, "Data bigger than Int32.max can't be handled by libnfs.")
        
        let handle = try self.handle.unwrap()
        var buffer = Array(data)
        let result = try context.async_await { (context, cbPtr) -> Int32 in
            nfs_pwrite_async(context, handle, offset, UInt64(buffer.count), &buffer, NFSContext.generic_handler, cbPtr)
        }
        
        return Int(result)
    }
    
    func fsync() throws {
        let handle = try self.handle.unwrap()
        try context.async_await { (context, cbPtr) -> Int32 in
            nfs_fsync_async(context, handle, NFSContext.generic_handler, cbPtr)
        }
    }
}
