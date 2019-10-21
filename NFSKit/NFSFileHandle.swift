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
    private var _handle: OpaquePointer?
    
    init(fileDescriptor: fileid3, on context: NFSContext) {
        self.context = context
    }
    
}
