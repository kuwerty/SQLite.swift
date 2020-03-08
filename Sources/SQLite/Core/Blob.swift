//
// SQLite.swift
// https://github.com/stephencelis/SQLite.swift
// Copyright © 2014-2015 Stephen Celis.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
import Foundation
#if SQLITE_SWIFT_STANDALONE
import sqlite3
#elseif SQLITE_SWIFT_SQLCIPHER
import SQLCipher
#elseif os(Linux)
import CSQLite
#else
import SQLite3
#endif

public struct Blob {

    public let bytes: [UInt8]

    public init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    public init(bytes: UnsafeRawPointer, length: Int) {
        let i8bufptr = UnsafeBufferPointer(start: bytes.assumingMemoryBound(to: UInt8.self), count: length)
        self.init(bytes: [UInt8](i8bufptr))
    }

    public func toHex() -> String {
        return bytes.map {
            ($0 < 16 ? "0" : "") + String($0, radix: 16, uppercase: false)
        }.joined(separator: "")
    }

}

extension Blob : CustomStringConvertible {

    public var description: String {
        return "x'\(toHex())'"
    }

}

extension Blob : Equatable {

}

public func ==(lhs: Blob, rhs: Blob) -> Bool {
    return lhs.bytes == rhs.bytes
}

public struct ZeroBlob {
  let count : UInt64

  public init(count:Int64) {
    self.count = UInt64(count)
  }

  public init(count:UInt64) {
    self.count = count
  }
}

public class BlobStream {
  var handle : OpaquePointer?
  var offset : UInt64 = 0
  public private(set) var size   : UInt64

  init(handle:OpaquePointer) {
    self.handle = handle

    self.size = UInt64(sqlite3_blob_bytes(handle))
  }

  deinit {
    close()
  }

  public func close() {
    if let handle = handle {
      sqlite3_blob_close(handle)

      self.handle = nil
    }
  }

  public func read(into data:inout Data) throws -> Int {
    guard let handle = handle else {
      throw Result.error(message: "Blob handle is closed", code: SQLITE_ERROR, statement: nil)
    }

    assert(offset <= size)

    if offset == size {
      return 0
    }

    return try data.withUnsafeMutableBytes { buffer in

      let remainingCount = size - offset

      let count = min(remainingCount, UInt64(buffer.count))

      let code = sqlite3_blob_read(handle, buffer.baseAddress, Int32(count), Int32(offset))

      if code != SQLITE_OK {
        throw Result.error(message: "Error reading SQLite blob: count:\(count) offset:\(offset) size:\(size)", code: code, statement: nil)
      }

      offset += count

      return Int(count)
    }
  }

  public func write(_ data:Data) throws {
    guard let handle = handle else {
      throw Result.error(message: "Blob handle is closed", code: SQLITE_ERROR, statement: nil)
    }

    try data.withUnsafeBytes { buffer in

      let code = sqlite3_blob_write(handle, buffer.baseAddress, Int32(buffer.count), Int32(offset))

      if code != SQLITE_OK {
        throw Result.error(message: "Error writing blob", code: code, statement: nil)
      }

      offset += UInt64(buffer.count)
    }
  }
}
