//
//  NFSFileHandle.swift
//  NFSKit
//
//  Created by xu.shuifeng on 2019/10/21.
//  Copyright © 2019 alexiscn. All rights reserved.
//

import Foundation
import nfs
import nfs.Raw

final class NFSFileHandle {
    
    struct SeekWhere: RawRepresentable {
        var rawValue: Int32
        
        static let set     = SeekWhere(rawValue: SEEK_SET)
        static let current = SeekWhere(rawValue: SEEK_CUR)
        static let end     = SeekWhere(rawValue: SEEK_END)
    }
    
    private var context: NFSContext
    private var _handle: UnsafeMutablePointer<nfsfh>?
    
    convenience init(forReadingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_RDONLY, on: context)
    }
    
    convenience init(forWritingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_WRONLY, on: context)
    }
    
    convenience init(forCreatingAndWritingAtPath path: String, on context: NFSContext) throws {
        try self.init(path, flags: O_WRONLY | O_CREAT | O_EXCL, on: context)
    }
    
    init(fileDescriptor: fileid3, on context: NFSContext) {
        self.context = context
        //var fileDescriptor = fileDescriptor
        //self._handle = getfh
    }
    
    private init(_ path: String, flags: Int32, on context: NFSContext) throws {
        let (_, cmddata) = try context.async_await(defaultError: .ENOENT) { (context, cbPtr) -> Int32 in
            nfs_open_async(context, path, flags, NFSContext.generic_handler, cbPtr)
        }
        guard let handle = cmddata?.bindMemory(to: UnsafeMutablePointer<nfsfh>.self, capacity: 1).pointee else {
            throw POSIXError(.ENOENT)
        }
        self.context = context
        self._handle = handle
    }
    
    func fstat() throws -> nfs_stat_64 {
        let handle = try tryHandle()
        let (_, cmddata) = try context.async_await(defaultError: .EBADF) { (context, cbPtr) -> Int32 in
            nfs_fstat64_async(context, handle, NFSContext.generic_handler, cbPtr)
        }
        guard let st = cmddata?.bindMemory(to: nfs_stat_64.self, capacity: 1).pointee else {
            throw POSIXError(.EIO)
        }
        return st
    }
    
    func ftruncate(toLength: UInt64) throws {
        let handle = try tryHandle()
        try context.async_await(defaultError: .EIO) { (context, cbPtr) -> Int32 in
            nfs_ftruncate_async(context, handle, toLength, NFSContext.generic_handler, cbPtr)
        }
    }
    
    var maxReadSize: Int {
        return Int(nfs_get_readmax(context.context))
    }
    
    /// This value allows softer streaming
    var optimizedReadSize: Int {
        return min(maxReadSize, 1048576)
    }
    
    @discardableResult
    func lseek(offset: Int64, whence: SeekWhere) throws -> Int64 {
        let handle = try tryHandle()
        let result = nfs_lseek(context.context, handle, offset, whence.rawValue, nil)
        if result < 0 {
            try POSIXError.throwIfError(result, description: context.error, default: .ESPIPE)
        }
        return Int64(result)
    }
    
    func read(length: Int = 0) throws -> Data {
        precondition(length <= UInt32.max, "Length bigger than UInt32.max can't be handled by libnfs.")
        let handle = try tryHandle()
        let count = length > 0 ? length: optimizedReadSize
        let (result, cmddata) = try context.async_await(defaultError: .EIO) { (context, cbPtr) -> Int32 in
            nfs_read_async(context, handle, UInt64(count), NFSContext.generic_handler, cbPtr)
        }
        guard let buffer = cmddata?.bindMemory(to: [UInt8].self, capacity: 1).pointee else {
            throw POSIXError(.EIO)
        }
        return Data(buffer.prefix(Int(result)))
    }
    
    func pread(offset: UInt64, length: Int = 0) throws -> Data {
        precondition(length <= UInt32.max, "Length bigger than UInt32.max can't be handled by libnfs.")
        let handle = try tryHandle()
        let count = length > 0 ? length: optimizedReadSize
        let (result, cmddata) = try context.async_await(defaultError: .EIO) { (context, cbPtr) -> Int32 in
            nfs_pread_async(context, handle, offset, UInt64(count), NFSContext.generic_handler, cbPtr)
        }
        guard let buffer = cmddata?.bindMemory(to: [UInt8].self, capacity: 1).pointee else {
            throw POSIXError(.EIO)
        }
        return Data(buffer.prefix(Int(result)))
    }
    
    var maxWriteSize: Int {
        return Int(nfs_get_writemax(context.context))
    }
    
    var optimizedWriteSize: Int {
        return min(maxWriteSize, 1048576)
    }
    
    func write(data: Data) throws -> Int {
        precondition(data.count <= Int32.max, "Data bigger than Int32.max can't be handled by libnfs.")
        let handle = try tryHandle()
        var buffer = Array(data)
        let (result, _) = try context.async_await(defaultError: .EBUSY) { (context, cbPtr) -> Int32 in
            nfs_write_async(context, handle, UInt64(data.count), &buffer, NFSContext.generic_handler, cbPtr)
        }
        return Int(result)
    }
    
    func pwrite(data: Data, offset: UInt64) throws -> Int {
        precondition(data.count <= Int32.max, "Data bigger than Int32.max can't be handled by libnfs.")
        let handle = try tryHandle()
        var buffer = Array(data)
        let (result, _) = try context.async_await(defaultError: .EBUSY) { (context, cbPtr) -> Int32 in
            nfs_pwrite_async(context, handle, offset, UInt64(data.count), &buffer, NFSContext.generic_handler, cbPtr)
        }
        return Int(result)
    }
    
    func fsync() throws {
        let handle = try tryHandle()
        try context.async_await(defaultError: .EIO) { (context, cbPtr) -> Int32 in
            nfs_fsync_async(context, handle, NFSContext.generic_handler, cbPtr)
        }
    }
    
}

extension NFSFileHandle {
    private func tryHandle() throws -> UnsafeMutablePointer<nfsfh> {
        guard let handle = _handle else {
            throw POSIXError(.EBADF, description: "NFS file is already closed.")
        }
        return handle
    }
}
