//
//  NFSClient.swift
//  NetworkFileSystemKit
//
//  Created by xu.shuifeng on 2019/10/17.
//  Copyright © 2019 alexiscn. All rights reserved.
//

import Foundation
import nfs

public typealias NFSCompletionHandler = ((_ error: Error?) -> Void)?

public typealias NFSReadProgressHandler = ((_ bytes: Int64, _ total: Int64) -> Bool)?

public typealias NFSWriteProgressHandler = ((_ bytes: Int64) -> Bool)?

public typealias NFSFileResourcesAttributesHandler = (_ result: Result<[FileAttributeKey: Any], Error>) -> Void

public typealias NFSURLResourcesAttributesHandler = (_ result: Result<[URLResourceKey: Any], Error>) -> Void

private typealias CopyProgressHandler = ((_ bytes: Int64, _ soFar: Int64, _ total: Int64) -> Bool)?

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
    }
    
    public func unmount() {
        
    }
    
    /// Enumerates directory contents in the give path.
    /// - Parameter path: path of directory to be enumerated.
    /// - Parameter recursive: subdirectories will enumerated if `true`.
    /// - Parameter completionHandler: closure will be run after enumerating is completed. Result is an array of `[URLResourceKey: Any]` which holds files' attributes. file name is stored in `.nameKey`.
    public func contentsOfDirectory(atPath path: String,
                                    recursive: Bool = false,
                                    completionHandler: @escaping (_ result: Result<[[URLResourceKey: Any]], Error>) -> Void) {
        with(completionHandler: completionHandler) { context in
            return try self.listDirectory(path: path, recursive: recursive)
        }
    }
    
    /// Returns a dictionary that describes the attributes of the mounted file system on which a given path resides.
    /// - Parameter path: Any pathname within the mounted file system.
    /// - Parameter completionHandler: closure will be run after fetching attributes is completed. A dictionary object that describes the attributes of the mounted file system on which path resides. See _File-System Attribute Keys_ for a description of the keys available in the dictionary.
    public func attributesOfFileSystem(forPath path: String,
                                       completionHandler: @escaping NFSFileResourcesAttributesHandler) {
        with(completionHandler: completionHandler) { context in
            let stat = try context.statvfs(path)
            var result: [FileAttributeKey: Any] = [:]
            
            result[.systemNumber] = NSNumber(value: stat.f_fsid)
            result[.systemNodes] = NSNumber(value: stat.f_files)
            result[.systemFreeNodes] = NSNumber(value: stat.f_ffree)
            let blockSize = UInt64(stat.f_bsize)
            if stat.f_blocks < UInt64.max / blockSize {
                result[.systemSize] = NSNumber(value: blockSize * stat.f_blocks)
                result[.systemFreeSize] = NSNumber(value: blockSize * stat.f_bavail)
            }
            return result
        }
    }
    
    /// Returns the attributes of the item at given path.
    /// - Parameter path: path of file to be enumerated.
    /// - Parameter completionHandler: closure will be run after enumerating is completed.  An dictionary with `URLResourceKey` as key which holds file's attributes.
    public func attributesOfItem(atPath path: String,
                                 completionHandler: @escaping NFSURLResourcesAttributesHandler) {
        with(completionHandler: completionHandler) { context in
            let stat = try context.stat(path.canonical)
            var result: [URLResourceKey: Any] = [:]
            let name = (path as NSString).lastPathComponent
            result[.nameKey] = name
            result[.pathKey] = (path as NSString).appendingPathComponent(name)
            // TODO
            return result
        }
    }
    
    /// Creates a new directory at given path.
    /// - Parameter path: path of new directory to be created.
    /// - Parameter completionHandler: closure will be run after operation is completed.
    public func createDirectory(atPath path: String,
                                completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.mkdir(path)
        }
    }
    
    /// Removes an existing directory at given path.
    /// - Parameter path: path of directory to be removed.
    /// - Parameter recusive: children items will be deleted if `true`.
    /// - Parameter completionHandler: closure will be run after operation is completed.
    public func removeDirectory(atPath path: String,
                                recusive: Bool,
                                completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) { context in
            if recusive {
                // To delete directory recursively, first we list directory contents recursively,
                // Then sort path descending which will put child files before containing directory,
                // Then we will unlink/rmdir every entry.
                //
                // This block will only delete children of directory, the path itself will removed after if block.
                let list = try self.listDirectory(path: path, recursive: true).sortedByName(.orderedDescending)
                for item in list {
                    guard let itemPath = item.filePath else { continue }
                    if item.fileType == URLFileResourceType.directory {
                        try context.rmdir(itemPath)
                    } else {
                        try context.unlink(itemPath)
                    }
                }
            }
            try context.rmdir(path)
        }
    }
    
    /// Removes an existing file at given path.
    /// - Parameter path: path of file to be removed.
    /// - Parameter completionHandler: closure will be run after operation is completed.
    public func removeFile(atPath path: String, completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.unlink(path)
        }
    }
    
    /// Truncates or extends the file represented by the path to a specified offset within the
    /// file and puts the file pointer at that position.
    /// If the file is extended (if offset is beyond the current end of file), the added characters are null bytes.
    /// - Parameter path: path of file to be truncated.
    /// - Parameter atOffset: final size of truncated file.
    /// - Parameter completionHandler: closure will be run after operation is completed.
    public func truncateFile(atPath path: String, atOffset: UInt64, completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.truncate(path, toLength: atOffset)
        }
    }
    
    /// Moves/Renames an existing file at given path to a new location.
    /// - Parameter path: path of file to be move.
    /// - Parameter toPath: new location of file.
    /// - Parameter completionHandler: closure will be run after operation is completed.
    public func moveItem(atPath path: String, toPath: String, completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.rename(path, to: toPath)
        }
    }
    
    
    /// Fetches whole data contents of a file. With reporting progress on about every 1MiB.
    /// - Parameter path: path of file to be fetched.
    /// - Parameter progressHandler: reports progress of recieved bytes count read and expected content length.
    /// - Parameter completionHandler: closure will be run after reading data is completed.
    public func contents(atPath path: String,
                         progressHandler: NFSReadProgressHandler,
                         completionHandler: @escaping (Result<Data, Error>) -> Void) {
        contents(atPath: path, range: 0..<Int64.max, progressHandler: progressHandler, completionHandler: completionHandler)
    }
    
    
    /// Fetches data contents of a file from an offset with specified length. With reporting progress on about every 1MiB.
    /// Note: If range's lowerBound is bigger than file's size, an empty `Data` will be returned.
    /// Note: If range's length exceeds file, returned data will be truncated to entire file content from given offset.
    /// - Parameter path: path of file to be fetched.
    /// - Parameter range: byte range that should be read, default value is whole file. e.g. `..<10` will read first ten bytes.
    /// - Parameter progressHandler: reports progress of recieved bytes count read and expected content length.
    /// - Parameter completionHandler: closure will be run after reading data is completed.
    public func contents<R: RangeExpression>(atPath path: String,
                                             range: R? = nil,
                                             progressHandler: NFSReadProgressHandler,
                                             completionHandler: @escaping (Result<Data, Error>) -> Void)
        where R.Bound: FixedWidthInteger {
        let range: Range<R.Bound> = range?.relative(to: 0..<R.Bound.max) ?? 0..<R.Bound.max
        let lower = Int64(exactly: range.lowerBound) ?? (Int64.max - 1)
        let upper = Int64(exactly: range.upperBound) ?? Int64.max
        let int64Range = lower..<upper
        
        with(completionHandler: completionHandler) { context in
            guard !int64Range.isEmpty else {
                return Data()
            }
            
            let stream = OutputStream.toMemory()
            try self.read(path: path, range: int64Range, to: stream, progressHandler: progressHandler)
            guard let data = stream.property(forKey: .dataWrittenToMemoryStreamKey) as? Data else {
                throw POSIXError(.ENOMEM, description: "Data missed from stream")
            }
            return data
        }
    }
    
    /// Copy files to a new location. With reporting progress on about every 1MiB.
    /// - Parameter path: path of file to be copied from.
    /// - Parameter toPath: path of new file to be copied to.
    /// - Parameter recursive: copies directory structure and files if path is directory.
    /// - Parameter progressHandler: reports progress of written bytes count so far and expected length of contents. User must return `true` if they want to continuing or `false` to abort copying.
    /// - Parameter completionHandler: closure will be run after copying is completed.
    public func copyItem(atPath path: String, toPath: String, recursive: Bool, progressHandler: NFSReadProgressHandler, completionHandler: NFSCompletionHandler) {
        
    }
    
    /// Uploads local file contents to a new location. With reporting progress on about every 1MiB.
    /// Note: given url must be local file url otherwise it will throw error.
    /// - Parameter url: url of a local file to be uploaded from.
    /// - Parameter toPath: path of new file to be uploaded to.
    /// - Parameter progressHandler: reports progress of written bytes count so far. User must return `true` if they want to continuing or `false` to abort copying.
    /// - Parameter completionHandler: closure will be run after uploading is completed.
    public func uploadItem(at url: URL, toPath: String, progressHandler: NFSWriteProgressHandler, completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) {
            guard try url.checkResourceIsReachable(), url.isFileURL, let stream = InputStream(url: url) else {
                throw POSIXError(.EIO, description: "Could not create Stream from given URL, or given URL is not a local file.")
            }
            
            try self.write(from: stream, toPath: toPath, progressHandler: progressHandler)
        }
    }
    
    
    /// Downloads file contents to a local url. With reporting progress on about every 1MiB.
    /// Note: if a file already exists on given url, This function will overwrite to that url.
    /// Note: given url must be local file url otherwise it will throw error.
    /// - Parameter path: path of file to be downloaded from.
    /// - Parameter url: url of a local file to be written to.
    /// - Parameter progressHandler: reports progress of written bytes count so farand expected length of contents. User must return `true` if they want to continuing or `false` to abort copying.
    /// - Parameter completionHandler: closure will be run after uploading is completed.
    public func downloadItem(atPath path: String,
                             to url: URL,
                             progressHandler: NFSReadProgressHandler,
                             completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) {
            guard url.isFileURL, let stream = OutputStream(url: url, append: false) else {
                throw POSIXError(.EIO, description: "Could not create Stream from given URL, or given URL is not a local file.")
            }
            try self.read(path: path, to: stream, progressHandler: progressHandler)
        }
    }
    
    
    /// Downloads file contents to a local url. With reporting progress on about every 1MiB.
    /// Note: if a file already exists on given url, This function will overwrite to that url.
    /// Note: given url must be local file url otherwise it will throw error.
    /// Important: Stream will be closed eventually if is not alrady opened.
    /// - Parameter path: path of file to be downloaded from.
    /// - Parameter stream: stream to be written to.
    /// - Parameter progressHandler: reports progress of written bytes count so farand expected length of contents. User must return `true` if they want to continuing or `false` to abort copying.
    /// - Parameter completionHandler: closure will be run after uploading is completed.
    public func downloadItem(atPath path: String,
                             to stream: OutputStream,
                             progressHandler: NFSReadProgressHandler,
                             completionHandler: NFSCompletionHandler) {
        with(completionHandler: completionHandler) {
            try self.read(path: path, to: stream, progressHandler: progressHandler)
        }
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
        context.timeout = _timeout
    }
    
    private func tryContext() throws -> NFSContext {
        guard let context = self.context else {
            throw POSIXError.init(.ENOTCONN, description: "NFS server not connected.")
        }
        return context
    }
    
    private func with(completionHandler: NFSCompletionHandler, handler: @escaping () throws -> Void) {
        executeBlock {
            do {
                try handler()
                completionHandler?(nil)
            } catch {
                completionHandler?(error)
            }
        }
    }
    
    private func with(completionHandler: NFSCompletionHandler,
                      handler: @escaping (_ context: NFSContext) throws -> Void) {
        executeBlock {
            do {
                let context = try self.tryContext()
                try handler(context)
                completionHandler?(nil)
            } catch {
                completionHandler?(error)
            }
        }
    }
    
    private func with<T>(completionHandler: @escaping (Result<T, Error>) -> Void,
                         handler: @escaping (_ context: NFSContext) throws -> T) {
        executeBlock {
            completionHandler(.init(catching: { () -> T in
                let context = try self.tryContext()
                return try handler(context)
            }))
        }
    }
}

// MARK: - File Operation
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
            let subDirectories = contents.filter { $0.fileType == .directory }
            for subDir in subDirectories {
                guard let path = subDir.filePath else { continue }
                contents.append(contentsOf: try listDirectory(path: path, recursive: true))
            }
        }
        return contents
    }
    
    private func copyFile(atPath path: String, toPath: String, progress: CopyProgressHandler) {
        
    }
    
    private func copyContentsOfFile(atPath path: String, toPath: String, progressHandler: CopyProgressHandler) throws -> Bool {
        let context = try tryContext()
        let fileRead = try NFSFileHandle(forReadingAtPath: path, on: context)
        let size = try Int64(fileRead.fstat().nfs_size)
        let fileWrite = try NFSFileHandle(forCreatingAndWritingAtPath: toPath, on: context)
        var shouldContinue = true
        while shouldContinue {
            let data = try fileRead.read()
            let written = try fileWrite.write(data: data)
            let offset = try fileRead.lseek(offset: 0, whence: .current)
            
            shouldContinue = progressHandler?(Int64(written), offset, size) ?? true
            shouldContinue = shouldContinue && !data.isEmpty
        }
        try fileWrite.fsync()
        return shouldContinue
    }
    
    private func read(path: String,
                      range: Range<Int64> = 0..<Int64.max,
                      to stream: OutputStream,
                      progressHandler: NFSReadProgressHandler) throws {
        let context = try tryContext()
        let file = try NFSFileHandle(forReadingAtPath: path, on: context)
        let fileSize = try Int64(file.fstat().nfs_size)
        let length = range.upperBound - range.lowerBound
        let size = min(length, fileSize - range.lowerBound)
        
        try stream.withOpenStream {
            var shouldContinue = true
            var sentBytes: Int64 = 0
            try file.lseek(offset: range.lowerBound, whence: .set)
            while shouldContinue {
                let prefCount = Int(min(Int64(file.optimizedReadSize), Int64(size - sentBytes)))
                guard prefCount > 0 else {
                    break
                }
                let data = try file.read(length: prefCount)
                if data.isEmpty {
                    break
                }
                let written = try stream.write(data)
                guard written == data.count else {
                    throw POSIXError(.EIO, description: "Inconsitency in reading from NFS file handle.")
                }
                sentBytes += Int64(written)
                shouldContinue = progressHandler?(sentBytes, size) ?? true
            }
        }
    }
    
    private func write(from stream: InputStream, toPath: String, chunkSize: Int = 0, progressHandler: NFSWriteProgressHandler) throws {
        let context = try tryContext()
        if (try? context.stat(toPath)) != nil {
            throw POSIXError(POSIXError.EEXIST, description: "File already exists.")
        }
        let file = try NFSFileHandle(forCreatingAndWritingAtPath: toPath, on: context)
        let size = chunkSize > 0 ? chunkSize: file.optimizedWriteSize
        var totalWritten: UInt64 = 0
        do {
            try stream.withOpenStream {
                while true {
                    var segment = try stream.readData(maxLength: size)
                    if segment.count == 0 {
                        break
                    }
                    totalWritten += UInt64(segment.count)
                    // For last part, we make it size equal with other chunks in order to prevent POLLHUP on some servers
                    if segment.count < file.optimizedWriteSize {
                        segment.count = file.optimizedWriteSize
                    }
                    let written = try file.write(data: segment)
                    if written != segment.count {
                        throw POSIXError(.EIO, description: "Inconsitency in writing to SMB file handle.")
                    }
                    var offset = try file.lseek(offset: 0, whence: .current)
                    if offset > totalWritten {
                        offset = Int64(totalWritten)
                    }
                    if let shouldContinue = progressHandler?(offset), !shouldContinue {
                        break
                    }
                }
            }
            try file.ftruncate(toLength: totalWritten)
            try file.fsync()
        } catch {
            try? context.unlink(toPath)
            throw error
        }
    }
    
}
