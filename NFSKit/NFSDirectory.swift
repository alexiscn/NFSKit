//
//  NFSDirectory.swift
//  NFSKit
//
//  Created by xushuifeng on 2019/10/20.
//  Copyright © 2019 alexiscn. All rights reserved.
//

import Foundation
import nfs

final class NFSDirectory: Collection {
    
    typealias Index = Int
    
    private var context: NFSContext
    private var handle: UnsafeMutablePointer<nfsdir>
    
    init(_ path: String, on context: NFSContext) throws {
        let (_, cmddata) = try context.async_await(defaultError: .ENOENT) { (context, cbPtr) -> Int32 in
            nfs_opendir_async(context, path, NFSContext.generic_handler, cbPtr)
        }
        
        guard let pointer = OpaquePointer(cmddata) else {
            throw POSIXError(.ENOTDIR)
        }
        let handle = UnsafeMutablePointer<nfsdir>(pointer)
        self.context = context
        self.handle = handle
    }
    
    deinit {
        
    }
    
    func makeIterator() -> AnyIterator<nfsdirent> {
        nfs_rewinddir(context.context, handle)
        return AnyIterator {
            return nfs_readdir(self.context.context, self.handle).move()
        }
    }
    
    var startIndex: Int {
        return 0
    }
    
    var endIndex: Int {
        return self.count
    }
    
    var count: Int {
        
        let currentPos = nfs_telldir(context.context, handle)
        defer {
            nfs_seekdir(context.context, handle, currentPos)
        }
        nfs_rewinddir(context.context, handle)
        var i = 0
        while nfs_readdir(context.context, handle) != nil {
            i += 1
        }
        return i
    }
    
    subscript(position: Int) -> nfsdirent {
        let currentPos = nfs_telldir(context.context, handle)
        nfs_seekdir(context.context, handle, 0)
        defer {
            nfs_seekdir(context.context, handle, currentPos)
        }
        return nfs_readdir(context.context, handle).move()
    }
    
    func index(after i: Int) -> Int {
        return i + 1
    }
}
