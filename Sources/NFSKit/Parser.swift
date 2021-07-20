//
//  Parser.swift
//  NFSKit
//
//  Created by alexiscn on 2021/7/18.
//

import Foundation
import nfs

struct Parser {
    
    static func toVoid(_ context: UnsafeMutablePointer<nfs_context>, _ dataPtr: UnsafeMutableRawPointer?) throws -> Void {
        return
    }
    
    static func toString(_ context: UnsafeMutablePointer<nfs_context>, _ dataPtr: UnsafeMutableRawPointer?) throws -> String {
        return try String(cString: dataPtr.unwrap().assumingMemoryBound(to: Int8.self))
    }
    
    static func toOpaquePointer(_ context: UnsafeMutablePointer<nfs_context>, _ dataPtr: UnsafeMutableRawPointer?) throws -> OpaquePointer {
        return try OpaquePointer(dataPtr.unwrap())
    }
    
    static func tostatvfs(_ context: UnsafeMutablePointer<nfs_context>, _ dataPtr: UnsafeMutableRawPointer?) throws -> statvfs {
        return try dataPtr.unwrap().assumingMemoryBound(to: statvfs.self).pointee
    }
    
    static func tostat64(_ context: UnsafeMutablePointer<nfs_context>, _ dataPtr: UnsafeMutableRawPointer?) throws -> nfs_stat_64 {
        return try dataPtr.unwrap().assumingMemoryBound(to: nfs_stat_64.self).pointee
    }
    
}
