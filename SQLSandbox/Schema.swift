//
//  Schema.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import Dependencies
import Foundation
import GRDB
import IssueReporting
import OSLog
import SQLiteData
import SwiftUI
import Synchronization

@Table
struct SampleTable: Hashable, Identifiable {
    @Column(primaryKey: true)
    let id: String
    var date: Date
    var event: String
    var pageType: String
    var sourceType: String
    var engagementType: String
    var device: String
    var platformVersion: String
    var territory: String
    var count: Int
    var uniqueCount: Int
}

private let logger = Logger(subsystem: "SQLSandbox", category: "Database")

// MARK: - Optimized UUIDv7 Generator

/// Pre-computed hex lookup table for fast byte-to-hex conversion
private let hexTable: [String] = (0...255).map { String(format: "%02x", $0) }

/// Generates a UUIDv7 string (time-ordered UUID for better B-tree insert performance)
/// Optimized version using:
/// - Pre-computed hex lookup table (avoids String(format:) overhead)
/// - Single 64-bit random call instead of 10x UInt8.random
/// - Direct string building
@inline(__always)
func generateUUIDv7() -> String {
    let timestamp = UInt64(Date().timeIntervalSince1970 * 1000)
    
    // Generate random bits efficiently (2x UInt64 = 128 bits, we need 80)
    var rng = SystemRandomNumberGenerator()
    let rand1 = rng.next()
    let rand2 = rng.next()
    
    // Build UUID bytes
    // Bytes 0-5: timestamp (48 bits)
    let b0 = UInt8((timestamp >> 40) & 0xFF)
    let b1 = UInt8((timestamp >> 32) & 0xFF)
    let b2 = UInt8((timestamp >> 24) & 0xFF)
    let b3 = UInt8((timestamp >> 16) & 0xFF)
    let b4 = UInt8((timestamp >> 8) & 0xFF)
    let b5 = UInt8(timestamp & 0xFF)
    
    // Bytes 6-7: version 7 + random
    let b6 = UInt8((rand1 & 0x0F) | 0x70)  // Version 7
    let b7 = UInt8((rand1 >> 8) & 0xFF)
    
    // Bytes 8-15: variant + random
    let b8 = UInt8(((rand1 >> 16) & 0x3F) | 0x80)  // Variant
    let b9 = UInt8((rand1 >> 24) & 0xFF)
    let b10 = UInt8((rand1 >> 32) & 0xFF)
    let b11 = UInt8((rand1 >> 40) & 0xFF)
    let b12 = UInt8((rand1 >> 48) & 0xFF)
    let b13 = UInt8((rand1 >> 56) & 0xFF)
    let b14 = UInt8(rand2 & 0xFF)
    let b15 = UInt8((rand2 >> 8) & 0xFF)
    
    // Fast string building using pre-computed hex table
    return hexTable[Int(b0)] + hexTable[Int(b1)] + hexTable[Int(b2)] + hexTable[Int(b3)] + "-" +
           hexTable[Int(b4)] + hexTable[Int(b5)] + "-" +
           hexTable[Int(b6)] + hexTable[Int(b7)] + "-" +
           hexTable[Int(b8)] + hexTable[Int(b9)] + "-" +
           hexTable[Int(b10)] + hexTable[Int(b11)] + hexTable[Int(b12)] + hexTable[Int(b13)] + hexTable[Int(b14)] + hexTable[Int(b15)]
}

func appDatabase() throws -> any DatabaseWriter {
    @Dependency(\.context) var context
    var configuration = Configuration()
    configuration.foreignKeysEnabled = true
    configuration.prepareDatabase { db in
        // Register UUIDv7 function for time-ordered UUIDs (better insert performance)
        let uuidv7 = DatabaseFunction("uuidv7", argumentCount: 0, pure: false) { _ in
            return generateUUIDv7()
        }
        db.add(function: uuidv7)
    }
    let database = try SQLiteData.defaultDatabase(configuration: configuration)
    logger.debug(
        """
        App database:
        open "\(database.path)"
        """
    )
    var migrator = DatabaseMigrator()
    #if DEBUG
        migrator.eraseDatabaseOnSchemaChange = true
    #endif
    migrator.registerMigration("Create initial tables") { db in
        // Create SampleTable with UUIDv7 primary key for CloudKit sync compatibility
        // UUIDv7 is time-ordered → sequential inserts → consistent performance
        // WITHOUT ROWID optimizes TEXT primary key performance by storing data
        // directly in the primary key B-tree instead of maintaining a separate rowid
        try #sql(
            """
            CREATE TABLE "sampleTables" (
              "id" TEXT PRIMARY KEY NOT NULL ON CONFLICT REPLACE DEFAULT (uuidv7()),
              "date" TEXT NOT NULL,
              "event" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "pageType" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "sourceType" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "engagementType" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "device" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "platformVersion" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "territory" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "count" INTEGER NOT NULL ON CONFLICT REPLACE DEFAULT 0,
              "uniqueCount" INTEGER NOT NULL ON CONFLICT REPLACE DEFAULT 0
            ) STRICT, WITHOUT ROWID
            """
        )
        .execute(db)
    }

    try migrator.migrate(database)

    return database
}
