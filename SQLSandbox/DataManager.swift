//
//  DataManager.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import Dependencies
import Foundation
import GRDB
import IssueReporting
import SQLiteData
import TabularData

struct DataManager {
    @Dependency(\.defaultDatabase) private var database

    static func generateTestDataFrame(count: Int = 1_000_000) -> DataFrame {
        let startDate = Date().addingTimeInterval(-Double(count))
        var dates: [Date] = []
        dates.reserveCapacity(count)
        for i in 0..<count {
            dates.append(Calendar.current.date(byAdding: .second, value: i, to: startDate)!)
        }

        var df = DataFrame()
        df.append(column: Column(name: "appTitle", contents: Array(repeating: "PERF.TEST.APP", count: count)))
        df.append(column: Column(name: "date", contents: dates))
        df.append(column: Column(name: "event", contents: Array(repeating: "Impression", count: count)))
        df.append(column: Column(name: "pageType", contents: Array(repeating: "Product Page", count: count)))
        df.append(column: Column(name: "sourceType", contents: Array(repeating: "Browse", count: count)))
        df.append(column: Column(name: "engagementType", contents: Array(repeating: "Tap", count: count)))
        df.append(column: Column(name: "device", contents: Array(repeating: "iPhone", count: count)))
        df.append(column: Column(name: "platformVersion", contents: Array(repeating: "iOS 18", count: count)))
        df.append(column: Column(name: "territory", contents: Array(repeating: "US", count: count)))
        df.append(column: Column(name: "count", contents: (0..<count).map { _ in Int.random(in: 1...3) }))
        df.append(column: Column(name: "uniqueCount", contents: (0..<count).map { _ in Int.random(in: 1...3) }))
        return df
    }

    @discardableResult
    func oldImportDataFrame(_ dataFrame: DataFrame) async throws -> Int {
        let df = dataFrame
        var importedCount = 0

        let dates = df["date", Date.self]
        let events = df["event", String.self]
        let pageTypes = df["pageType", String.self]
        let sourceTypes = df["sourceType", String.self]
        let engagementTypes = df["engagementType", String.self]
        let devices = df["device", String.self]
        let platformVersions = df["platformVersion", String.self]
        let territories = df["territory", String.self]
        let counts = df["count", Int.self]
        let uniqueCounts = df["uniqueCount", Int.self]

        // ✅ PRAGMAs must run OUTSIDE the write transaction
        try await database.writeWithoutTransaction { db in
            try db.execute(sql: "PRAGMA journal_mode = WAL;")
            try db.execute(sql: "PRAGMA synchronous = NORMAL;")
            try db.execute(sql: "PRAGMA temp_store = MEMORY;")
            try db.execute(sql: "PRAGMA cache_size = -20000;")
        }

        try await database.write { db in
            let sql = """
            INSERT INTO "sampleTables"
            ("date", "event", "pageType", "sourceType", "engagementType", "device", "platformVersion", "territory", "count", "uniqueCount")
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            let stmt = try db.makeStatement(sql: sql)
            let rowCount = df.rows.count

            for rowIndex in 0..<rowCount {
                guard
                    let date = dates[rowIndex],
                    let event = events[rowIndex],
                    let pageType = pageTypes[rowIndex],
                    let sourceType = sourceTypes[rowIndex],
                    let engagementType = engagementTypes[rowIndex],
                    let device = devices[rowIndex],
                    let platformVersion = platformVersions[rowIndex],
                    let territory = territories[rowIndex],
                    let count = counts[rowIndex],
                    let uniqueCount = uniqueCounts[rowIndex]
                else {
                    reportIssue("Invalid data at row \(rowIndex)")
                    continue
                }

                try stmt.execute(arguments: [
                    date,
                    event,
                    pageType,
                    sourceType,
                    engagementType,
                    device,
                    platformVersion,
                    territory,
                    count,
                    uniqueCount
                ])

                importedCount &+= 1
            }
        }

        return importedCount
    }

    @discardableResult
    func importDataFrame(_ dataFrame: DataFrame) async throws -> Int {
        let df = dataFrame
        var importedCount = 0

        let dates = df["date", Date.self]
        let events = df["event", String.self]
        let pageTypes = df["pageType", String.self]
        let sourceTypes = df["sourceType", String.self]
        let engagementTypes = df["engagementType", String.self]
        let devices = df["device", String.self]
        let platformVersions = df["platformVersion", String.self]
        let territories = df["territory", String.self]
        let counts = df["count", Int.self]
        let uniqueCounts = df["uniqueCount", Int.self]

        let rowCount = df.rows.count
        let chunkSize = 10_000

        try await database.write { db in
            var start = 0

            while start < rowCount {
                let end = min(start + chunkSize, rowCount)

                try SampleTable
                    .insert {
                        for rowIndex in start..<end {
                            if
                                let date = dates[rowIndex],
                                let event = events[rowIndex],
                                let pageType = pageTypes[rowIndex],
                                let sourceType = sourceTypes[rowIndex],
                                let engagementType = engagementTypes[rowIndex],
                                let device = devices[rowIndex],
                                let platformVersion = platformVersions[rowIndex],
                                let territory = territories[rowIndex],
                                let count = counts[rowIndex],
                                let uniqueCount = uniqueCounts[rowIndex]
                            {
                                SampleTable.Draft(
                                    date: date,
                                    event: event,
                                    pageType: pageType,
                                    sourceType: sourceType,
                                    engagementType: engagementType,
                                    device: device,
                                    platformVersion: platformVersion,
                                    territory: territory,
                                    count: count,
                                    uniqueCount: uniqueCount
                                )
                            }
                        }
                    }
                    .execute(db)

                start = end
            }
        }

        importedCount = rowCount

        return importedCount
    }
}
