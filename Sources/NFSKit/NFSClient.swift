//
//  NFSClient.swift
//  NFSKit
//
//  Created by alexiscn on 2021/7/17.
//

import Foundation
import nfs

public class NFSClient: NSObject {
    
    public typealias CompletionHandler = ((_ error: Error?) -> Void)?
    public typealias ReadProgressHandler = ((_ bytes: Int64, _ total: Int64) -> Bool)?
    public typealias WriteProgressHandler = ((_ bytes: Int64) -> Bool)?
    fileprivate typealias CopyProgressHandler = ((_ bytes: Int64, _ soFar: Int64, _ total: Int64) -> Bool)?
    
    public let url: URL
    
    
    /// The guid.
    public private(set) var gid: Int32 = 0
    public private(set) var uid: Int32 = 0
    
    fileprivate var context: NFSContext?
    fileprivate let q: DispatchQueue
    fileprivate let connectLock = NSLock()
    fileprivate let operationLock = NSCondition()
    fileprivate var operationCount: Int = 0
    fileprivate var _timeout: TimeInterval
    
    /**
     The timeout interval to use when doing an operation until getting response. Default value is 60 seconds.
     Set this to 0 or negative value in order to disable it.
     */
    @objc
    open var timeout: TimeInterval {
        get {
            return context?.timeout ?? _timeout
        }
        set {
            _timeout = newValue
            context?.timeout = newValue
        }
    }
    
    /// Initializes a NFSClient class with given url
    /// - Parameter url: The nfs url, should has with nfs:// scheme
    /// - Throws: error
    public init?(url: URL) throws {
        let hostLabel = url.host.map({ "_" + $0 }) ?? ""
        self.q = DispatchQueue(label: "nfs_queue\(hostLabel)", qos: .default, attributes: .concurrent)
        self.url = url
        self._timeout = 60
        self.context = try NFSContext(timeout: _timeout)
    }
    
    
    /// Mount the export
    /// - Parameters:
    ///   - export: export name to be mounted.
    ///   - completionHandler: closure will be run after enumerating is completed.
    open func connect(export: String, completionHandler: @escaping ((_ error: Error?) -> Void)) {
        with(completionHandler: completionHandler) {
            self.connectLock.lock()
            defer { self.connectLock.unlock() }
            if self.context == nil || self.context?.fileDescriptor == -1 || self.context?.export != export {
                self.context = try self.connnect(exportName: export)
            }
            
            do {
                let (uid, gid) = try self.context.unwrap().autoguid()
                self.uid = uid
                self.gid = gid
            } catch {
                self.context = try self.connnect(exportName: export)
            }
        }
    }
    
    /// Mount the export
    /// - Parameters:
    ///   - export: export name to be mounted.
    @available(macOS 10.15, *)
    open func connect(export: String) async -> Error? {
        return await withCheckedContinuation { continuation in
            connect(export: export) { result in
                if let error = result {
                    continuation.resume(returning: error)
                } else {
                    continuation.resume(returning: result)
                }
            }
        }
    }
    
    /// Umount export.
    /// - Parameters:
    ///   - export: export to be umount
    ///   - gracefully: waits until all queued operations are done before disconnecting from server. Default value is `false`.
    ///   - completionHandler: closure will be run after enumerating is completed.
    open func disconnect(export: String, gracefully: Bool = false, completionHandler: CompletionHandler = nil) {
        q.async {
            do {
                self.connectLock.lock()
                defer { self.connectLock.unlock() }
                if gracefully {
                    self.operationLock.lock()
                    while self.operationCount > 0 {
                        self.operationLock.wait()
                    }
                    self.operationLock.unlock()
                }
                try self.context?.disconnect()
                self.context = nil
                completionHandler?(nil)
            } catch {
                completionHandler?(error)
            }
        }
    }
    
    /// Umount export.
    /// - Parameters:
    ///   - export: export to be umount
    ///   - gracefully: waits until all queued operations are done before disconnecting from server. Default value is `false`.
    @available(macOS 10.15, *)
    open func disconnect(export: String, gracefully: Bool = false) async throws{
        return await withCheckedContinuation { continuation in
            disconnect(export: export, gracefully: gracefully) { result in
                continuation.resume()
            }
        }
    }
    
    
    /// List the exports of NFS server.
    /// - Parameter completionHandler: closure will be run after enumerating is completed.
    open func listExports(completionHandler: @escaping (_ result: Result<[String], Error>) -> Void) {
        queue {
            do {
                let result = try self.context?.getexports(server: self.url.host ?? "") ?? []
                completionHandler(.success(result))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    /// List the exports of NFS server.
    @available(macOS 10.15, *)
    open func listExports() async throws -> Result<[String], Error> {
        return await withCheckedContinuation{continuation in
            listExports() { result in
                continuation.resume(returning: result)
            }
        }
    }
    
    /**
     Enumerates directory contents in the give path.
     
     - Parameters:
       - atPath: path of directory to be enumerated.
       - completionHandler: closure will be run after enumerating is completed.
       - recursive: subdirectories will enumerated if `true`.
       - result: An array of `[URLResourceKey: Any]` which holds files' attributes. file name is stored in `.nameKey`.
     */
    open func contentsOfDirectory(atPath path: String, recursive: Bool = false,
                                  completionHandler: @escaping (_ result: Result<[[URLResourceKey: Any]], Error>) -> Void) {
        with(completionHandler: completionHandler) { context in
            return try self.listDirectory(context: context, path: path, recursive: recursive)
        }
    }
    
    /**
     Enumerates directory contents in the give path.
     
     - Parameters:
       - atPath: path of directory to be enumerated.
       - recursive: subdirectories will enumerated if `true`.
       - result: An array of `[URLResourceKey: Any]` which holds files' attributes. file name is stored in `.nameKey`.
     */
    @available(macOS 10.15, *)
    open func contentsOfDirectory(atPath path: String, recursive: Bool = false) async -> Result<[[URLResourceKey: Any]], Error> {
        return await withCheckedContinuation{ continuation in
            contentsOfDirectory(atPath: path, recursive: recursive) { result in
                continuation.resume(returning: result)
            }
        }
    }
    
    /**
     Returns a dictionary that describes the attributes of the mounted file system on which a given path resides.
     
     - Parameters:
       - forPath: Any pathname within the mounted file system.
       - completionHandler: closure will be run after fetching attributes is completed.
       - result: A dictionary object that describes the attributes of the mounted file system on which path resides.
           See _File-System Attribute Keys_ for a description of the keys available in the dictionary.
     */
    open func attributesOfFileSystem(forPath path: String,
                                     completionHandler: @escaping (_ result: Result<[FileAttributeKey: Any], Error>) -> Void) {
        with(completionHandler: completionHandler) { context in
            // This exactly matches implementation of Swift Foundation.
            let stat = try context.statvfs(path.canonical)
            var result = [FileAttributeKey: Any]()
            let blockSize = UInt64(stat.f_bsize)
            // NSNumber allows to cast to any number type, but it is unsafe to cast to types with lower bitwidth
            result[.systemNumber] = NSNumber(value: stat.f_fsid)
            if stat.f_blocks < UInt64.max / blockSize {
                result[.systemSize] = NSNumber(value: blockSize * UInt64(stat.f_blocks))
                result[.systemFreeSize] = NSNumber(value: blockSize * UInt64(stat.f_bavail))
            }
            result[.systemNodes] = NSNumber(value: stat.f_files)
            result[.systemFreeNodes] = NSNumber(value: stat.f_ffree)
            return result
        }
    }
    
    /**
     Returns a dictionary that describes the attributes of the mounted file system on which a given path resides.
     
     - Parameters:
       - forPath: Any pathname within the mounted file system.
       - result: A dictionary object that describes the attributes of the mounted file system on which path resides.
           See _File-System Attribute Keys_ for a description of the keys available in the dictionary.
     */
    @available(macOS 10.15, *)
    open func attributesOfFileSystem(forPath path: String) async -> Result<[FileAttributeKey: Any], Error> {
        return await withCheckedContinuation { continuation in
            attributesOfFileSystem(forPath: path) { result in
                continuation.resume(returning: result)
            }
        }
    }
    
    /**
     Returns the attributes of the item at given path.
     
     - Parameters:
       - atPath: path of file to be enumerated.
       - completionHandler: closure will be run after enumerating is completed.
       - result: An dictionary with `URLResourceKey` as key which holds file's attributes.
     */
    open func attributesOfItem(atPath path: String,
                               completionHandler: @escaping (_ result: Result<[URLResourceKey: Any], Error>) -> Void) {
        with(completionHandler: completionHandler) { context in
            let stat = try context.stat(path.canonical)
            var result = [URLResourceKey: Any]()
            let name = (path as NSString).lastPathComponent
            result[.nameKey] = name
            result[.pathKey] = (path as NSString).appendingPathComponent(name)
            self.populateResourceValue(&result, stat: stat)
            return result
        }
    }
    
    /**
     Returns the attributes of the item at given path.
     
     - Parameters:
       - atPath: path of file to be enumerated.
       - result: An dictionary with `URLResourceKey` as key which holds file's attributes.
     */
    @available(macOS 10.15, *)
    open func attributesOfItem(atPath path: String) async -> Result<[URLResourceKey: Any], Error> {
        return await withCheckedContinuation { continuation in
            attributesOfItem(atPath: path) { result in
                continuation.resume(returning: result)
            }
        }
    }
    
    /**
    Returns the path of the item pointed to by a symbolic link.
    
    - Parameters:
      - atPath: The path of a file or directory.
      - completionHandler: closure will be run after reading link is completed.
      - result: An String object containing the path of the directory or file to which the symbolic link path refers.
                If the symbolic link is specified as a relative path, that relative path is returned.
    */
    open func destinationOfSymbolicLink(atPath path: String,
                                        completionHandler: @escaping (_ result: Result<String, Error>) -> Void) {
        with(completionHandler: completionHandler) { context in
            return try context.readlink(path)
        }
    }
    
    /**
    Returns the path of the item pointed to by a symbolic link.
    
    - Parameters:
      - atPath: The path of a file or directory.
      - result: An String object containing the path of the directory or file to which the symbolic link path refers.
                If the symbolic link is specified as a relative path, that relative path is returned.
    */
    @available(macOS 10.15, *)
    open func destinationOfSymbolicLink(atPath path: String) async -> Result<String, Error> {
        return await withCheckedContinuation { continuation in
            destinationOfSymbolicLink(atPath: path) { result in
                continuation.resume(returning: result)
            }
        }
    }
    
    /**
     Creates a new directory at given path.
     
     - Parameters:
       - atPath: path of new directory to be created.
       - completionHandler: closure will be run after operation is completed.
     */
    open func createDirectory(atPath path: String, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.mkdir(path)
        }
    }
    
    /**
     Creates a new directory at given path.
     
     - Parameters:
       - atPath: path of new directory to be created.
     */
    @available(macOS 10.15, *)
    open func createDirectory(atPath path: String) async {
        return await withCheckedContinuation{ continuation in
            createDirectory(atPath: path) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Removes an existing directory at given path.
     
     - Parameters:
       - atPath: path of directory to be removed.
       - recursive: children items will be deleted if `true`.
       - completionHandler: closure will be run after operation is completed.
     */
    open func removeDirectory(atPath path: String, recursive: Bool, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try self.removeDirectory(context: context, path: path, recursive: recursive)
        }
    }
    
    /**
     Removes an existing directory at given path.
     
     - Parameters:
       - atPath: path of directory to be removed.
       - recursive: children items will be deleted if `true`.
     */
    @available(macOS 10.15, *)
    open func removeDirectory(atPath path: String, recursive: Bool) async {
        return await withCheckedContinuation { continuation in
            removeDirectory(atPath: path, recursive: recursive) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Removes an existing file at given path.
     
     - Parameters:
       - atPath: path of file to be removed.
       - completionHandler: closure will be run after operation is completed.
     */
    open func removeFile(atPath path: String, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.unlink(path)
        }
    }
    
    /**
     Removes an existing file at given path.
     
     - Parameters:
       - atPath: path of file to be removed.
     */
    @available(macOS 10.15, *)
    open func removeFile(atPath path: String) async {
        return await withCheckedContinuation { continuation in
            removeFile(atPath: path) { result in
                continuation.resume()
            }
        }
    }
    
    /**
        Removes an existing file or directory at given path.
        
        - Parameters:
          - atPath: path of file or directory to be removed.
          - completionHandler: closure will be run after operation is completed.
        */
    open func removeItem(atPath path: String, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            let mode = try context.stat(path).nfs_mode
            switch mode & UInt64(S_IFMT) {
            case UInt64(S_IFDIR):
                try self.removeDirectory(context: context, path: path, recursive: true)
            case UInt64(S_IFREG), UInt64(S_IFLNK):
                try context.unlink(path)
            default:
                break
            }
        }
    }
    
    @available(macOS 10.15, *)
    open func removeItem(atPath path: String) async {
        return await withCheckedContinuation { continuation in
            removeItem(atPath: path) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Truncates or extends the file represented by the path to a specified offset within the file and
     puts the file pointer at that position.
     
     If the file is extended (if offset is beyond the current end of file), the added characters are null bytes.
     
     - Parameters:
       - atPath: path of file to be truncated.
       - atOffset: final size of truncated file.
       - completionHandler: closure will be run after operation is completed.
     */
    open func truncateFile(atPath path: String, atOffset: UInt64, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.truncate(path, toLength: atOffset)
        }
    }
    
    /**
     Truncates or extends the file represented by the path to a specified offset within the file and
     puts the file pointer at that position.
     
     If the file is extended (if offset is beyond the current end of file), the added characters are null bytes.
     
     - Parameters:
       - atPath: path of file to be truncated.
       - atOffset: final size of truncated file.
     */
    @available(macOS 10.15, *)
    open func truncateFile(atPath path: String, atOffset: UInt64) async {
        return await withCheckedContinuation { continuation in
            truncateFile(atPath: path, atOffset: atOffset) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Moves/Renames an existing file at given path to a new location.
     
     - Parameters:
       - atPath: path of file to be move.
       - toPath: new location of file.
       - completionHandler: closure will be run after operation is completed.
     */
    open func moveItem(atPath path: String, toPath: String, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try context.rename(path, to: toPath)
        }
    }
    
    /**
     Moves/Renames an existing file at given path to a new location.
     
     - Parameters:
       - atPath: path of file to be move.
       - toPath: new location of file.
     */
    @available(macOS 10.15, *)
    open func moveItem(atPath path: String, toPath: String) async {
        return await withCheckedContinuation { continuation in
            moveItem(atPath: path, toPath: toPath) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Fetches whole data contents of a file. With reporting progress on about every 1MiB.
     
     - Parameters:
       - atPath: path of file to be fetched.
       - progress: reports progress of recieved bytes count read and expected content length.
           User must return `true` if they want to continuing or `false` to abort reading.
       - bytes: recieved bytes count.
       - total: expected content length.
       - completionHandler: closure will be run after reading data is completed.
       - result: a `Data` object which contains file contents.
     */
    open func contents(atPath path: String, progress: ReadProgressHandler,
                       completionHandler: @escaping (_ result: Result<Data, Error>) -> Void) {
        contents(atPath: path, range: 0..<Int64.max, progress: progress, completionHandler: completionHandler)
    }
    
    /**
     Fetches whole data contents of a file. With reporting progress on about every 1MiB.
     
     - Parameters:
       - atPath: path of file to be fetched.
       - progress: reports progress of recieved bytes count read and expected content length.
           User must return `true` if they want to continuing or `false` to abort reading.
       - bytes: recieved bytes count.
       - total: expected content length.
       - result: a `Data` object which contains file contents.
     */
    @available(macOS 10.15, *)
    open func contents(atPath path: String, progress: ReadProgressHandler) async {
        return await withCheckedContinuation { continuation in
            contents(atPath: path, progress: progress) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Fetches data contents of a file from an offset with specified length. With reporting progress
     on about every 1MiB.
     
     - Note: If range's lowerBound is bigger than file's size, an empty `Data` will be returned.
             If range's length exceeds file, returned data will be truncated to entire file content from given offset.
     
     - Parameters:
       - atPath: path of file to be fetched.
       - range: byte range that should be read, default value is whole file. e.g. `..<10` will read first ten bytes.
       - progress: reports progress of recieved bytes count read and expected content length.
           User must return `true` if they want to continuing or `false` to abort reading.
       - bytes: recieved bytes count.
       - total: expected content length.
       - completionHandler: closure will be run after reading data is completed.
       - result: a `Data` object which contains file contents.
     */
    open func contents<R: RangeExpression>(atPath path: String, range: R? = nil, progress: ReadProgressHandler,
                                           completionHandler: @escaping (_ result: Result<Data, Error>) -> Void)
        where R.Bound: FixedWidthInteger
    {
        let range: Range<R.Bound> = range?.relative(to: 0..<R.Bound.max) ?? 0..<R.Bound.max
        let lower = Int64(exactly: range.lowerBound) ?? (Int64.max - 1)
        let upper = Int64(exactly: range.upperBound) ?? Int64.max
        let int64Range = lower..<upper
        
        with(completionHandler: completionHandler) { context in
            guard !int64Range.isEmpty else {
                return Data()
            }
            
            let stream = OutputStream.toMemory()
            try self.read(context: context, path: path, range: int64Range, to: stream, progress: progress)
            return try (stream.property(forKey: .dataWrittenToMemoryStreamKey) as? Data).unwrap()
        }
    }
    
    /**
     Fetches data contents of a file from an offset with specified length. With reporting progress
     on about every 1MiB.
     
     - Note: If range's lowerBound is bigger than file's size, an empty `Data` will be returned.
             If range's length exceeds file, returned data will be truncated to entire file content from given offset.
     
     - Parameters:
       - atPath: path of file to be fetched.
       - range: byte range that should be read, default value is whole file. e.g. `..<10` will read first ten bytes.
       - progress: reports progress of recieved bytes count read and expected content length.
           User must return `true` if they want to continuing or `false` to abort reading.
       - bytes: recieved bytes count.
       - total: expected content length.
       - result: a `Data` object which contains file contents.
     */
    @available(macOS 10.15, iOS 13, *)
    open func contents<R: RangeExpression>(atPath path: String, range: R? = nil, progress: ReadProgressHandler) async -> Result<Data, Error>
        where R.Bound: FixedWidthInteger
    {
        return await withCheckedContinuation { continuation in
            contents(atPath: path, range: range, progress: progress) { result in
                continuation.resume(returning: result)
            }
        }
    }
    
    /**
     Streams data contents of a file from an offset with specified length. With reporting data and progress
     on about every 1MiB.
     
     - Parameters:
       - atPath: path of file to be fetched.
       - offset: first byte of file to be read, starting from zero.
       - fetchedData: returns data portion fetched and recieved bytes count read and expected content length.
           User must return `true` if they want to continuing or `false` to abort reading.
       - offset: offset of first byte of data portion in file.
       - total: expected content length.
       - data: data portion which read from server.
       - completionHandler: closure will be run after reading data is completed.
     */
    open func contents(atPath path: String, offset: Int64 = 0,
                       fetchedData: @escaping ((_ offset: Int64, _ total: Int64, _ data: Data) -> Bool),
                       completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            let file = try NFSFileHandle(forReadingAtPath: path, on: context)
            let size = try Int64(file.fstat().nfs_size)
            
            var shouldContinue = true
            try file.lseek(offset: offset, whence: .set)
            while shouldContinue {
                let offset = try file.lseek(offset: 0, whence: .current)
                let data = try file.read()
                if data.isEmpty {
                    break
                }
                shouldContinue = fetchedData(offset, size, data)
            }
        }
    }
    
    /**
     Streams data contents of a file from an offset with specified length. With reporting data and progress
     on about every 1MiB.
     
     - Parameters:
       - atPath: path of file to be fetched.
       - offset: first byte of file to be read, starting from zero.
       - fetchedData: returns data portion fetched and recieved bytes count read and expected content length.
           User must return `true` if they want to continuing or `false` to abort reading.
       - offset: offset of first byte of data portion in file.
       - total: expected content length.
       - data: data portion which read from server.
       - completionHandler: closure will be run after reading data is completed.
     */
    @available(macOS 10.15, iOS 13, *)
    open func contents(atPath path: String, offset: Int64 = 0, fetchedData: @escaping ((_ offset: Int64, _ total: Int64, _ data: Data) -> Bool)) async
    {
        return await withCheckedContinuation { continuation in
            contents(atPath: path, offset: offset, fetchedData: fetchedData) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Creates and writes data to file. With reporting progress on about every 1MiB.
     
     - Note: Data saved in server maybe truncated when completion handler returns error.
     
     - Parameters:
       - data: data that must be written to file. You can pass either `Data`, `[UInt8]` or `NSData` object.
       - toPath: path of file to be written.
       - progress: reports progress of written bytes count so far.
           User must return `true` if they want to continuing or `false` to abort writing.
       - bytes: written bytes count.
       - completionHandler: closure will be run after writing is completed.
     */
    open func write<DataType: DataProtocol>(data: DataType, toPath path: String, progress: WriteProgressHandler,
                                            completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try self.write(context: context, from: InputStream(data: Data(data)), toPath: path, progress: progress)
        }
    }
    
    /**
     Creates and writes data to file. With reporting progress on about every 1MiB.
     
     - Note: Data saved in server maybe truncated when completion handler returns error.
     
     - Parameters:
       - data: data that must be written to file. You can pass either `Data`, `[UInt8]` or `NSData` object.
       - toPath: path of file to be written.
       - progress: reports progress of written bytes count so far.
           User must return `true` if they want to continuing or `false` to abort writing.
       - bytes: written bytes count.
     */
    @available(macOS 10.15, iOS 13, *)
    open func write<DataType: DataProtocol>(data: DataType, toPath path: String, progress: WriteProgressHandler) async {
        return await withCheckedContinuation { continuation in
            write(data: data, toPath: path, progress: progress) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Creates and writes input stream to file. With reporting progress on about every 1MiB.
     
     - Note: Data saved in server maybe truncated when completion handler returns error.
     
     - Important: Stream will be closed eventually if is not already opened when passed.
     
     - Parameters:
       - stream: input stream that provides data to be written to file.
       - toPath: path of file to be written.
       - chunkSize: optimized chunk size to read from stream. Default value is abount 1MB.
       - progress: reports progress of written bytes count so far.
           User must return `true` if they want to continuing or `false` to abort writing.
       - bytes: written bytes count.
       - completionHandler: closure will be run after writing is completed.
     */
    open func write(stream: InputStream, toPath path: String, chunkSize: Int = 0, progress: WriteProgressHandler,
                    completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try self.write(context: context, from: stream, toPath: path, chunkSize: chunkSize, progress: progress)
        }
    }
    
    /**
     Creates and writes input stream to file. With reporting progress on about every 1MiB.
     
     - Note: Data saved in server maybe truncated when completion handler returns error.
     
     - Important: Stream will be closed eventually if is not already opened when passed.
     
     - Parameters:
       - stream: input stream that provides data to be written to file.
       - toPath: path of file to be written.
       - chunkSize: optimized chunk size to read from stream. Default value is abount 1MB.
       - progress: reports progress of written bytes count so far.
           User must return `true` if they want to continuing or `false` to abort writing.
       - bytes: written bytes count.
     */
    @available(macOS 10.15, iOS 13, *)
    open func write(stream: InputStream, toPath path: String, chunkSize: Int = 0, progress: WriteProgressHandler) async {
        return await withCheckedContinuation { continuation in
            write(stream: stream, toPath: path, chunkSize: chunkSize, progress: progress) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Uploads local file contents to a new location. With reporting progress on about every 1MiB.
     
     - Note: given url must be local file url otherwise it will throw error.
     
     - Parameters:
       - at: url of a local file to be uploaded from.
       - toPath: path of new file to be uploaded to.
       - progress: reports progress of written bytes count so far.
           User must return `true` if they want to continuing or `false` to abort copying.
       - completionHandler: closure will be run after uploading is completed.
     */
    open func uploadItem(at url: URL, toPath: String, progress: WriteProgressHandler, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            guard try url.checkResourceIsReachable(), url.isFileURL, let stream = InputStream(url: url) else {
                throw POSIXError(.EIO, description: "Could not create Stream from given URL, or given URL is not a local file.")
            }
            
            try self.write(context: context, from: stream, toPath: toPath, progress: progress)
        }
    }
    
    /**
     Uploads local file contents to a new location. With reporting progress on about every 1MiB.
     
     - Note: given url must be local file url otherwise it will throw error.
     
     - Parameters:
       - at: url of a local file to be uploaded from.
       - toPath: path of new file to be uploaded to.
       - progress: reports progress of written bytes count so far.
           User must return `true` if they want to continuing or `false` to abort copying.
     */
    @available(macOS 10.15, iOS 13, *)
    open func uploadItem(at url: URL, toPath: String, progress: WriteProgressHandler) async {
        return await withCheckedContinuation { continuation in
            uploadItem(at: url, toPath: toPath, progress: progress) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Downloads file contents to a local url. With reporting progress on about every 1MiB.
     
     - Note: if a file already exists on given url, This function will overwrite to that url.
     
     - Note: given url must be local file url otherwise it will throw error.
     
     - Parameters:
       - atPath: path of file to be downloaded from.
       - at: url of a local file to be written to.
       - progress: reports progress of written bytes count so farand expected length of contents.
           User must return `true` if they want to continuing or `false` to abort copying.
       - completionHandler: closure will be run after uploading is completed.
     */
    open func downloadItem(atPath path: String, to url: URL, progress: ReadProgressHandler, completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            guard url.isFileURL, let stream = OutputStream(url: url, append: false) else {
                throw POSIXError(.EIO, description: "Could not create Stream from given URL, or given URL is not a local file.")
            }
            try self.read(context: context, path: path, to: stream, progress: progress)
        }
    }
    
    /**
     Downloads file contents to a local url. With reporting progress on about every 1MiB.
     
     - Note: if a file already exists on given url, This function will overwrite to that url.
     
     - Note: given url must be local file url otherwise it will throw error.
     
     - Parameters:
       - atPath: path of file to be downloaded from.
       - at: url of a local file to be written to.
       - progress: reports progress of written bytes count so farand expected length of contents.
           User must return `true` if they want to continuing or `false` to abort copying.
     */
    @available(macOS 10.15, iOS 13, *)
    open func downloadItem(atPath path: String, to url: URL, progress: ReadProgressHandler) async {
        return await withCheckedContinuation { continuation in
            downloadItem(atPath: path, to: url, progress: progress) { result in
                continuation.resume()
            }
        }
    }
    
    /**
     Downloads file contents to a local url. With reporting progress on about every 1MiB.
     
     - Note: if a file already exists on given url, This function will overwrite to that url.
     
     - Note: given url must be local file url otherwise it will throw error.
     
     - Important: Stream will be closed eventually if is not alrady opened.
     
     - Parameters:
       - atPath: path of file to be downloaded from.
       - at: url of a local file to be written to.
       - progress: reports progress of written bytes count so farand expected length of contents.
         User must return `true` if they want to continuing or `false` to abort copying.
       - completionHandler: closure will be run after uploading is completed.
     */
    open func downloadItem(atPath path: String, to stream: OutputStream, progress: ReadProgressHandler,
                           completionHandler: CompletionHandler) {
        with(completionHandler: completionHandler) { context in
            try self.read(context: context, path: path, to: stream, progress: progress)
        }
    }
    
    /**
     Downloads file contents to a local url. With reporting progress on about every 1MiB.
     
     - Note: if a file already exists on given url, This function will overwrite to that url.
     
     - Note: given url must be local file url otherwise it will throw error.
     
     - Important: Stream will be closed eventually if is not alrady opened.
     
     - Parameters:
       - atPath: path of file to be downloaded from.
       - at: url of a local file to be written to.
       - progress: reports progress of written bytes count so farand expected length of contents.
         User must return `true` if they want to continuing or `false` to abort copying.
     */
    @available(macOS 10.15, iOS 13, *)
    open func downloadItem(atPath path: String, to stream: OutputStream, progress: ReadProgressHandler) async {
        return await withCheckedContinuation { continuation in
            downloadItem(atPath: path, to: stream, progress: progress) { result in
                continuation.resume()
            }
        }
    }
}

extension NFSClient {
    
    private func listDirectory(context: NFSContext, path: String, recursive: Bool) throws -> [[URLResourceKey: Any]] {
        var contents = [[URLResourceKey: Any]]()
        
        let dir = try NFSDirectory(path, on: context)
        for ent in dir {
            let name = String(cString: ent.name)
            if [".", ".."].contains(name) { continue }
            var result = [URLResourceKey: Any]()
            result[.nameKey] = name
            result[.pathKey] = (path as NSString).appendingPathComponent(name)
            populateResourceValue(&result, ent: ent)
            contents.append(result)
        }
        
        if recursive {
            let subDirectories = contents.filter { $0.isDirectory }
            
            for subDir in subDirectories {
                contents.append(contentsOf: try listDirectory(context: context, path: subDir.path.unwrap(), recursive: true))
            }
        }
        
        return contents
    }
    
    fileprivate func recursiveCopyIterator(context: NFSContext, fromPath path: String, toPath: String, recursive: Bool, progress: ReadProgressHandler,
                                           handle: (_ context: NFSContext, _ path: String, _ toPath: String, _ progress: CopyProgressHandler) throws -> Bool) throws {
        let stat = try context.stat(path)
        
        if stat.nfs_mode == UInt64(S_IFDIR) {
            try context.mkdir(toPath)

            let list = try listDirectory(context: context, path: path, recursive: recursive).sortedByPath(.orderedAscending)
            let overallSize = list.overallSize

            var totalCopied: Int64 = 0
            for item in list {
                let itemPath = try item.path.unwrap()
                let destPath = itemPath.replacingOccurrences(of: path, with: toPath, options: .anchored)
                if item.isDirectory {
                    try context.mkdir(destPath)
                } else {
                    let shouldContinue = try handle(context, itemPath, destPath, {
                        (bytes, _, _) -> Bool in
                        totalCopied += bytes
                        return progress?(totalCopied, overallSize) ?? true
                    })
                    if !shouldContinue {
                        break
                    }
                }
            }
        } else {
            _ = try handle(context, path, toPath, { (_, soFar, total) -> Bool in
                progress?(soFar, total) ?? true
            })
        }
    }
    
    fileprivate func copyContentsOfFile(context: NFSContext, fromPath path: String, toPath: String, progress: CopyProgressHandler) throws -> Bool {
        let fileRead = try NFSFileHandle(forReadingAtPath: path, on: context)
        let size = try Int64(fileRead.fstat().nfs_size)
        let fileWrite = try NFSFileHandle(forCreatingIfNotExistsAtPath: toPath, on: context)
        var shouldContinue = true
        while shouldContinue {
            let data = try fileRead.read()
            let written = try fileWrite.write(data: data)
            let offset = try fileRead.lseek(offset: 0, whence: .current)
            
            shouldContinue = progress?(Int64(written), offset, size) ?? true
            shouldContinue = shouldContinue && !data.isEmpty
        }
        try fileWrite.fsync()
        return shouldContinue
    }
    
    fileprivate func removeDirectory(context: NFSContext, path: String, recursive: Bool) throws {
        if recursive {
            // To delete directory recursively, first we list directory contents recursively,
            // Then sort path descending which will put child files before containing directory,
            // Then we will unlink/rmdir every entry.
            //
            // This block will only delete children of directory, the path itself will removed after if block.
            let list = try self.listDirectory(context: context, path: path, recursive: true).sortedByPath(.orderedDescending)
            
            for item in list {
                let itemPath = try item.path.unwrap()
                if item.isDirectory {
                    try context.rmdir(itemPath)
                } else {
                    try context.unlink(itemPath)
                }
            }
        }
        
        try context.rmdir(path)
    }
    
    fileprivate func read(context: NFSContext, path: String, range: Range<Int64> = 0..<Int64.max,
                          to stream: OutputStream, progress: ReadProgressHandler) throws {
        let file = try NFSFileHandle(forReadingAtPath: path, on: context)
        let filesize = try Int64(file.fstat().nfs_size)
        let length = range.upperBound - range.lowerBound
        let size = min(length, filesize - range.lowerBound)
        
        try stream.withOpenStream {
            var shouldContinue = true
            var sent: Int64 = 0
            try file.lseek(offset: range.lowerBound, whence: .set)
            while shouldContinue {
                let prefCount = Int(min(Int64(file.optimizedReadSize), Int64(size - sent)))
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
                sent += Int64(written)
                shouldContinue = progress?(sent, size) ?? true
            }
        }
    }
    
    fileprivate func write(context: NFSContext, from stream: InputStream, toPath: String,
                           chunkSize: Int = 0, progress: WriteProgressHandler) throws {
        let file = try NFSFileHandle(forCreatingIfNotExistsAtPath: toPath, on: context)
        let chunkSize = chunkSize > 0 ? chunkSize : file.optimizedWriteSize
        var totalWritten: UInt64 = 0
        
        do {
            try stream.withOpenStream {
                while true {
                    var segment = try stream.readData(maxLength: chunkSize)
                    if segment.count == 0 {
                        break
                    }
                    totalWritten += UInt64(segment.count)
                    // For last part, we make it size equal with other chunks in order to prevent POLLHUP on some servers
                    if segment.count < chunkSize {
                        segment.count = chunkSize
                    }
                    let written = try file.write(data: segment)
                    if written != segment.count {
                        throw POSIXError(.EIO, description: "Inconsitency in writing to NFS file handle.")
                    }
                    
                    var offset = try file.lseek(offset: 0, whence: .current)
                    if offset > totalWritten {
                        offset = Int64(totalWritten)
                    }
                    if let shouldContinue = progress?(offset), !shouldContinue {
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
    
    fileprivate func populateResourceValue(_ dic: inout [URLResourceKey: Any], ent: nfsdirent) {
        dic[.fileSizeKey] = NSNumber(value: ent.size)
        dic[.linkCountKey] = NSNumber(value: ent.nlink)
        dic[.documentIdentifierKey] = NSNumber(value: ent.uid)
        
        switch ent.type {
        case NF3REG.rawValue:
            dic[.fileResourceTypeKey] = URLFileResourceType.regular
        case NF3DIR.rawValue:
            dic[.fileResourceTypeKey] = URLFileResourceType.directory
        case NF3LNK.rawValue:
            dic[.fileResourceTypeKey] = URLFileResourceType.symbolicLink
        default:
            dic[.fileResourceTypeKey] = URLFileResourceType.unknown
        }
        dic[.isDirectoryKey] = NSNumber(value: ent.type == NF3DIR.rawValue)
        dic[.isRegularFileKey] = NSNumber(value: ent.type == NF3REG.rawValue)
        dic[.isSymbolicLinkKey] = NSNumber(value: ent.type == NF3LNK.rawValue)
        dic[.contentModificationDateKey] = Date(timespec(tv_sec: ent.mtime.tv_sec, tv_nsec: Int(ent.mtime_nsec)))
        dic[.attributeModificationDateKey] = Date(timespec(tv_sec: ent.ctime.tv_sec, tv_nsec: Int(ent.ctime_nsec)))
        dic[.contentAccessDateKey] = Date(timespec(tv_sec: ent.atime.tv_sec, tv_nsec: Int(ent.atime_nsec)))
        dic[.creationDateKey] = Date(timespec(tv_sec: ent.ctime.tv_sec, tv_nsec: Int(ent.ctime_nsec)))
    }
    
    fileprivate func populateResourceValue(_ dic: inout [URLResourceKey: Any], stat: nfs_stat_64) {
        dic[.fileSizeKey] = NSNumber(value: stat.nfs_size)
        dic[.linkCountKey] = NSNumber(value: stat.nfs_nlink)
        dic[.documentIdentifierKey] = NSNumber(value: stat.nfs_ino)
        
        switch stat.nfs_mode {
        case UInt64(S_IFREG):
            dic[.fileResourceTypeKey] = URLFileResourceType.regular
        case UInt64(S_IFDIR):
            dic[.fileResourceTypeKey] = URLFileResourceType.directory
        case UInt64(S_IFLNK):
            dic[.fileResourceTypeKey] = URLFileResourceType.symbolicLink
        default:
            dic[.fileResourceTypeKey] = URLFileResourceType.unknown
        }
        dic[.isDirectoryKey] = NSNumber(value: stat.nfs_mode == UInt64(S_IFDIR))
        dic[.isRegularFileKey] = NSNumber(value: stat.nfs_mode == UInt64(S_IFREG))
        dic[.isSymbolicLinkKey] = NSNumber(value: stat.nfs_mode == UInt64(S_IFLNK))
        dic[.contentModificationDateKey] = Date(timespec(tv_sec: Int(stat.nfs_mtime), tv_nsec: Int(stat.nfs_mtime_nsec)))
        dic[.attributeModificationDateKey] = Date(timespec(tv_sec: Int(stat.nfs_ctime), tv_nsec: Int(stat.nfs_ctime_nsec)))
        dic[.contentAccessDateKey] = Date(timespec(tv_sec: Int(stat.nfs_atime), tv_nsec: Int(stat.nfs_atime_nsec)))
        dic[.creationDateKey] = Date(timespec(tv_sec: Int(stat.nfs_ctime), tv_nsec: Int(stat.nfs_ctime_nsec)))
    }
}

extension NFSClient {
    
    private func queue(_ closure: @escaping () -> Void) {
        self.operationLock.lock()
        self.operationCount += 1
        self.operationLock.unlock()
        q.async {
            closure()
            self.operationLock.lock()
            self.operationCount -= 1
            self.operationLock.broadcast()
            self.operationLock.unlock()
        }
    }
    
    fileprivate func connnect(exportName: String) throws -> NFSContext {
        let context = try NFSContext(timeout: _timeout)
        self.context = context
        context.timeout = timeout
        let server = url.host! + (url.port.map { ":\($0)" } ?? "")
        try context.connect(server: server, export: exportName)
        return context
    }
    
    private func with(completionHandler: CompletionHandler, handler: @escaping () throws -> Void) {
        queue {
            do {
                try handler()
                completionHandler?(nil)
            } catch {
                completionHandler?(error)
            }
        }
    }
    
    private func with(completionHandler: CompletionHandler, handler: @escaping (_ context: NFSContext) throws -> Void) {
        queue {
            do {
                try handler(self.context.unwrap())
                completionHandler?(nil)
            } catch {
                completionHandler?(error)
            }
        }
    }
    
    private func with<T>(completionHandler: @escaping(Result<T, Error>) -> Void,
                         handler: @escaping (_ context: NFSContext) throws -> T) {
        queue {
            completionHandler(.init(catching: { () -> T in
                return try handler(self.context.unwrap())
            }))
        }
    }
    
    fileprivate func with<T>(exportName: String, completionHandler: @escaping (Result<T, Error>) -> Void,
                             handler: @escaping (_ context: NFSContext) throws -> T) {
        queue {
            do {
                let context = try self.connnect(exportName: exportName)
                defer { try? context.disconnect() }
                
                let result = try handler(context)
                completionHandler(.success(result))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
}
