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
       
        try await database.write { db in
            var insertedRows = 0
            try SampleTable
                .insert {
                    for rowIndex in 0..<rowCount {
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
            importedCount = insertedRows
        }
        
        return importedCount
    }
}
