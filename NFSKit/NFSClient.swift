//
//  NFSClient.swift
//  NetworkFileSystemKit
//
//  Created by xu.shuifeng on 2019/10/17.
//  Copyright © 2019 alexiscn. All rights reserved.
//

import Foundation
import nfs

public class NFSClient {
    
    public let url: URL
    
    private let queue: DispatchQueue
    private var context: NFSContext?
    private let connectLock = NSLock()
    private let operationLock = NSCondition()
    private var operationCount: Int = 0
    
    private var _timeout: TimeInterval
    public var timeout: TimeInterval {
        get { return context?.timeout ?? _timeout }
        set {
            _timeout = newValue
            context?.timeout = newValue
        }
    }
    
    public init(url: URL) {
        self.url = url
        let hostLabel = url.host.map({ "_" + $0 }) ?? ""
        self.queue = DispatchQueue(label: "nfs_queue\(hostLabel)", qos: .default, attributes: .concurrent)
        self._timeout = 60.0
    }
    
    public func mount(exportname: String, completion: @escaping (_ error: Error?) -> Void) {
        
        func establishContext() throws {
            let context = try NFSContext(timeout: _timeout)
            self.context = context
            setupContext(context)
            //try context.mount(server: <#T##String#>, exportname: exportname)
        }
        
//        executeBlock {
//            do {
//
//            } catch {
//                completion(error)
//            }
//        }
    }
    
    public func unmount() {
        
    }
    public func contentsOfDirectory(atPath path: String,
                                    recursive: Bool = false,
                                    completionHandler: @escaping (_ result: Result<[[URLResourceKey: Any]], Error>) -> Void) {
        
    }
}

public extension NFSClient {
    
    private func executeBlock(_ closure: @escaping () -> Void) {
        operationLock.lock()
        operationCount += 1
        operationLock.unlock()
        queue.async {
            closure()
            self.operationLock.lock()
            self.operationCount -= 1
            self.operationLock.broadcast()
            self.operationLock.unlock()
        }
    }
    
    private func setupContext(_ context: NFSContext) {
        
    }
    
    private func tryContext() throws -> NFSContext {
        guard let context = self.context else {
            throw POSIXError.init(.ENOTCONN, description: "NFS server not connected.")
        }
        return context
    }
    
}

extension NFSClient {
    
//    private func populateResourceValue(_ dict: inout [URLResourceKey: Any], stat: nfs_stat_64) {
//        dict[.contentModificationDateKey] = Date(timespec(tv_sec: Int(stat.nfs_mtime), tv_nsec: Int(stat.nfs_mtime_nsec)))
//        dict[.attributeModificationDateKey] = Date(timespec(tv_sec: Int(stat.nfs_ctime), tv_nsec: Int(stat.nfs_ctime_nsec)))
//        dict[.contentAccessDateKey] = Date(timespec(tv_sec: Int(stat.nfs_atime), tv_nsec: Int(stat.nfs_atime_nsec)))
//        dict[.creationDateKey] = Date(timespec(tv_sec: Int(stat.nfs_ctime), tv_nsec: Int(stat.nfs_ctime_nsec)))
//    }
    
    private func listDirectory(path: String, recursive: Bool) throws -> [[URLResourceKey: Any]] {
        let context = try tryContext()
        var contents = [[URLResourceKey: Any]]()
        let dir = try NFSDirectory(path.canonical, on: context)
        for entry in dir {
            guard let name = String(utf8String: entry.name) else { continue }
            if [".", ".."].contains(name) { continue }
            var result = [URLResourceKey: Any]()
            result[.nameKey] = name
            result[.pathKey] = (path as NSString).appendingPathComponent(name)
            result[.fileSizeKey] = NSNumber(value: entry.size)
            result[.linkCountKey] = NSNumber(value: entry.nlink)
            result[.documentIdentifierKey] = NSNumber(value: entry.inode)
            //result[.contentModificationDateKey] = Date(timespec(tv_sec: Int(entry.mtime), tv_nsec: Int(entry.time_nsec)))
            
            contents.append(result)
        }
        if recursive {
            
        }
        return contents
    }
    
}
