# NFSKit

A Swift Wrapper around [libnfs](https://github.com/sahlberg/libnfs), code heavily borrowed from [AMSMB2](https://github.com/amosavian/AMSMB2). Thanks [amosavian](https://github.com/amosavian) and [sahlberg](https://github.com/sahlberg) for the great work.

# Install

NFSKit only supports installation through Swift Package Manager.

```bash
dependencies: [
    .package(url: "https://github.com/alexiscn/NFSKit.git", branch: "main")
]
```

# Get started

Just read inline help to find what each function does. It's straightforward. It's thread safe and any queue.

```swift

class NFSBrowser {
    
    var client: NFSClient?
    
    // url: nfs://xxx.xxx.xxx.xxx
    init?(url: URL) throws {
        client = try NFSClient(url: url)
    }
    
    func listExports(handler: @escaping (Result<[String], Error>) -> Void) {
        client?.listExports(completionHandler: handler)
    }
    
    func mount(export: String, completion: @escaping (Result<Void, Error>) -> Void) {
        client?.connect(export: export) { error in
            DispatchQueue.main.async {
                if let error = error {
                    completion(.failure(error))
                } else {
                    completion(.success(()))
                }
            }
        }
    }

    func listDirectory(at path: String) {
        client?.contentsOfDirectory(atPath: path) { result in
            switch result {
            case .success(let items):
                for entry in items {
                    print("name:", entry[.nameKey] as! String,
                          ", path:", entry[.pathKey] as! String,
                          ", type:", entry[.fileResourceTypeKey] as! URLFileResourceType,
                          ", size:", entry[.fileSizeKey] as! Int64,
                          ", modified:", entry[.contentModificationDateKey] as! Date,
                          ", created:", entry[.creationDateKey] as! Date)
                }
            case .failure(let error):
                print(error)
            }
        }
    }

    func moveItem(atPath path: String, to toPath: String) {
        client?.moveItem(atPath: path, toPath: toPath) { error in
            if let error = error {
                print(error)
            }
        }
    }

    func removeItem(atPath path: String) {
        client?.removeItem(atPath: path) { error in
            if let error = error {
                print(error)
            }
        }
    }
    
    func downloadItem(atPath path: String) {
        let filePath = NSTemporaryDirectory().appending("temp.data")
        let fileURL = URL(fileURLWithPath: filePath)
        let progress = Progress(totalUnitCount: 0)
        client?.downloadItem(atPath: path, to: fileURL) { bytes, total in
            progress.totalUnitCount = total
            progress.completedUnitCount = bytes
            print(progress.fractionCompleted)
            return true
        } completionHandler: { error in
            if let error = error {
                print(error)
            }
        }
    }
    
    func upload(data: Data, toPath: String) {
        let progress = Progress(totalUnitCount: Int64(data.count))
        client?.write(data: data, toPath: toPath) { (uploaded) -> Bool in
            progress.completedUnitCount = uploaded
            print(progress.fractionCompleted)
            return true
        } completionHandler: { error in
            if let error = error {
                print(error)
            }
        }
    }
}
```

# License

While NFSKit source code shipped with project is MIT licensed, but libnfs which is `LGPL v2.1`.
