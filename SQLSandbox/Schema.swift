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
    #if DEBUG
      db.trace(options: .profile) {
        if context == .live {
          logger.debug("\($0.expandedDescription)")
        } else {
          print("\($0.expandedDescription)")
        }
      }
    #endif
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

    if context != .live {
//      try db.seedSampleData()
    }
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

// Extension for seeding sample data
#if DEBUG
extension Database {
  func seedSampleData() throws {
    @Dependency(\.date.now) var now
    @Dependency(\.uuid) var uuid
    
    let appIDs = (0...2).map { _ in uuid() }
    let sampleIDs = (0...10).map { _ in uuid() }
    
    try seed {
      DBApp(id: appIDs[0], title: "Mobile App")
      DBApp(id: appIDs[1], title: "Web App")
      DBApp(id: appIDs[2], title: "Desktop App")
      
      // Sample analytics data
      SampleTable(
        id: sampleIDs[0],
        date: now,
        AppID: appIDs[0],
        event: "app_launch",
        pageType: "main",
        sourceType: "direct",
        engagementType: "active",
        device: "iPhone",
        platformVersion: "iOS 17.0",
        territory: "US",
        count: 15,
        uniqueCount: 12
      )
      
      SampleTable(
        id: sampleIDs[1],
        date: now.addingTimeInterval(-3600),
        AppID: appIDs[0],
        event: "page_view",
        pageType: "detail",
        sourceType: "search",
        engagementType: "passive",
        device: "iPad",
        platformVersion: "iOS 17.1",
        territory: "DE",
        count: 25,
        uniqueCount: 18
      )
      
      SampleTable(
        id: sampleIDs[2],
        date: now.addingTimeInterval(-7200),
        AppID: appIDs[1],
        event: "button_click",
        pageType: "checkout",
        sourceType: "navigation",
        engagementType: "active",
        device: "Desktop",
        platformVersion: "macOS 14.0",
        territory: "UK",
        count: 8,
        uniqueCount: 6
      )
      
      SampleTable(
        id: sampleIDs[3],
        date: now.addingTimeInterval(-10800),
        AppID: appIDs[1],
        event: "form_submit",
        pageType: "contact",
        sourceType: "external",
        engagementType: "active",
        device: "Desktop",
        platformVersion: "Windows 11",
        territory: "CA",
        count: 3,
        uniqueCount: 3
      )
      
      SampleTable(
        id: sampleIDs[4],
        date: now.addingTimeInterval(-14400),
        AppID: appIDs[2],
        event: "download",
        pageType: "resources",
        sourceType: "social",
        engagementType: "passive",
        device: "Desktop",
        platformVersion: "macOS 13.6",
        territory: "AU",
        count: 12,
        uniqueCount: 9
      )
      
      SampleTable(
        id: sampleIDs[5],
        date: now.addingTimeInterval(-18000),
        AppID: appIDs[0],
        event: "scroll",
        pageType: "feed",
        sourceType: "direct",
        engagementType: "passive",
        device: "Android",
        platformVersion: "Android 14",
        territory: "JP",
        count: 45,
        uniqueCount: 23
      )
    }
  }
}
#endif
