//
//  DataManager.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import Dependencies
import Foundation
import IssueReporting
import SQLiteData
import TabularData

struct DataManager {
    @Dependency(\.defaultDatabase) private var database
    
    @discardableResult
    func importDataFrame(_ dataFrame: DataFrame) async throws -> Int {
        var importedCount = 0
        
        try await database.write { db in
            for rowIndex in 0 ..< dataFrame.rows.count {
                
                guard
                    let date = dataFrame["date", Date.self][rowIndex],
                    let event = dataFrame["event", String.self][rowIndex],
                    let pageType = dataFrame["pageType", String.self][rowIndex],
                    let sourceType = dataFrame["sourceType", String.self][rowIndex],
                    let engagementType = dataFrame["engagementType", String.self][rowIndex],
                    let device = dataFrame["device", String.self][rowIndex],
                    let platformVersion = dataFrame["platformVersion", String.self][rowIndex],
                    let territory = dataFrame["territory", String.self][rowIndex],
                    let count = dataFrame["count", Int.self][rowIndex],
                    let uniqueCount = dataFrame["uniqueCount", Int.self][rowIndex]
                else {
                    reportIssue("Invalid data at row \(rowIndex)")
                    continue
                }
                
                try SampleTable.insert {
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
                .execute(db)
                
                importedCount += 1
            }
        }
        
        return importedCount
    }
}
