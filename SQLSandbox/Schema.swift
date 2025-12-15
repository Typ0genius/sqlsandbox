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

/// Generates a UUIDv7 string (time-ordered UUID for better B-tree insert performance)
/// Format: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
/// - First 48 bits: Unix timestamp in milliseconds (time-ordered!)
/// - Version 7 indicator
/// - Random bits for uniqueness within same millisecond
private func generateUUIDv7() -> String {
    let timestamp = UInt64(Date().timeIntervalSince1970 * 1000)
    
    // Timestamp bytes (48 bits = 6 bytes)
    var bytes = [UInt8](repeating: 0, count: 16)
    bytes[0] = UInt8((timestamp >> 40) & 0xFF)
    bytes[1] = UInt8((timestamp >> 32) & 0xFF)
    bytes[2] = UInt8((timestamp >> 24) & 0xFF)
    bytes[3] = UInt8((timestamp >> 16) & 0xFF)
    bytes[4] = UInt8((timestamp >> 8) & 0xFF)
    bytes[5] = UInt8(timestamp & 0xFF)
    
    // Random bytes for the rest
    for i in 6..<16 {
        bytes[i] = UInt8.random(in: 0...255)
    }
    
    // Set version to 7 (0111 in high nibble of byte 6)
    bytes[6] = (bytes[6] & 0x0F) | 0x70
    
    // Set variant to RFC 4122 (10xx in high bits of byte 8)
    bytes[8] = (bytes[8] & 0x3F) | 0x80
    
    // Format as UUID string
    return String(format: "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                  bytes[0], bytes[1], bytes[2], bytes[3],
                  bytes[4], bytes[5],
                  bytes[6], bytes[7],
                  bytes[8], bytes[9],
                  bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15])
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
