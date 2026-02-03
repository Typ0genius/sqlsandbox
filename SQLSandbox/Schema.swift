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
    let id: UUID
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

private func uuidv7String() -> String {
    let timestampMs = UInt64(Date().timeIntervalSince1970 * 1000)
    var bytes = [UInt8](repeating: 0, count: 16)

    bytes[0] = UInt8((timestampMs >> 40) & 0xFF)
    bytes[1] = UInt8((timestampMs >> 32) & 0xFF)
    bytes[2] = UInt8((timestampMs >> 24) & 0xFF)
    bytes[3] = UInt8((timestampMs >> 16) & 0xFF)
    bytes[4] = UInt8((timestampMs >> 8) & 0xFF)
    bytes[5] = UInt8(timestampMs & 0xFF)

    var rng = SystemRandomNumberGenerator()
    var random = [UInt8](repeating: 0, count: 10)
    for index in 0..<random.count {
        random[index] = UInt8.random(in: UInt8.min...UInt8.max, using: &rng)
    }

    bytes[6] = 0x70 | (random[0] & 0x0F) // Version 7
    bytes[7] = random[1]
    bytes[8] = 0x80 | (random[2] & 0x3F) // Variant 1 (RFC 4122)
    bytes[9] = random[3]
    bytes[10] = random[4]
    bytes[11] = random[5]
    bytes[12] = random[6]
    bytes[13] = random[7]
    bytes[14] = random[8]
    bytes[15] = random[9]

    let uuid = UUID(uuid: (
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15]
    ))

    return uuid.uuidString
}

func appDatabase() throws -> any DatabaseWriter {
    @Dependency(\.context) var context
    var configuration = Configuration()
    configuration.foreignKeysEnabled = true
    configuration.prepareDatabase { db in
        // Register a UUID generator for default primary keys.
        db.add(function: DatabaseFunction("uuid", pure: false) { _ in
            uuidv7String()
        })
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
        // Create SampleTable
        try #sql(
            """
            CREATE TABLE "sampleTables" (
              "id" TEXT PRIMARY KEY NOT NULL ON CONFLICT REPLACE DEFAULT (uuid()),
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
            ) STRICT
            """
        )
        .execute(db)
    }

    try migrator.migrate(database)

    return database
}
