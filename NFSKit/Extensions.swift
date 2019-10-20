//
//  Extensions.swift
//  NetworkFileSystemKit
//
//  Created by xu.shuifeng on 2019/10/17.
//  Copyright © 2019 alexiscn. All rights reserved.
//

import Foundation

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

extension Date {
    init(_ timespec: timespec) {
        self.init(timeIntervalSince1970: TimeInterval(timespec.tv_sec) + TimeInterval(timespec.tv_nsec / 1000) / TimeInterval(USEC_PER_SEC))
    }
}

extension String {
    var canonical: String {
        return trimmingCharacters(in: .init(charactersIn: "/\\"))
    }
}
