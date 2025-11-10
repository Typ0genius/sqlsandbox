//
//  Schema.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import Dependencies
import Foundation
import IssueReporting
import OSLog
import SQLiteData
import SwiftUI
import Synchronization

@Table
struct DBApp: Hashable, Identifiable {
    let id: UUID
    var title = ""
}

@Table
struct SampleTable: Hashable, Identifiable {
    let id: UUID
    var date: Date
    var AppID: DBApp.ID
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

func appDatabase() throws -> any DatabaseWriter {
    @Dependency(\.context) var context
    var configuration = Configuration()
    configuration.foreignKeysEnabled = true
    configuration.prepareDatabase { db in
//    try db.attachMetadatabase()
        db.add(function: $handleReminderStatusUpdate)
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
        // Create DBApp table
        try #sql(
            """
            CREATE TABLE "dbApps" (
              "id" TEXT PRIMARY KEY NOT NULL ON CONFLICT REPLACE DEFAULT (uuid()),
              "title" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT ''
            ) STRICT
            """
        )
        .execute(db)

        // Create SampleTable
        try #sql(
            """
            CREATE TABLE "sampleTables" (
              "id" TEXT PRIMARY KEY NOT NULL ON CONFLICT REPLACE DEFAULT (uuid()),
              "date" TEXT NOT NULL,
              "AppID" TEXT NOT NULL REFERENCES "dbApps"("id") ON DELETE CASCADE,
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

    try database.write { db in
        // Ensure there's always at least one DBApp (similar to RemindersList logic)
        try DBApp.createTemporaryTrigger(
            after: .delete { _ in
                DBApp.insert {
                    DBApp.Draft(title: "Default App")
                }
            } when: { _ in
                !DBApp.exists()
            }
        )
        .execute(db)
    }

    return database
}

// Update the handleReminderStatusUpdate function for the new schema
let sampleTableMutex = Mutex<Task<Void, any Error>?>(nil)
@DatabaseFunction
func handleReminderStatusUpdate() {
    sampleTableMutex.withLock {
        $0?.cancel()
        $0 = Task {
            @Dependency(\.defaultDatabase) var database
            @Dependency(\.continuousClock) var clock
            try await clock.sleep(for: .seconds(5))
            try await database.write { db in
                // Example: Clean up old sample data or perform maintenance
                // You can customize this based on your needs
                try SampleTable
                    .where { $0.count.eq(0) }
                    .delete()
                    .execute(db)
            }
        }
    }
}
