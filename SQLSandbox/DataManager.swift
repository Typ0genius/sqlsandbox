//
//  DataManager.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import Foundation
import TabularData
import Dependencies
import SQLiteData
import IssueReporting

struct DataManager {
    @Dependency(\.defaultDatabase) private var database
    
    @discardableResult
    func importDataFrame(_ dataFrame: DataFrame, appID: UUID? = nil) async throws -> Int {
        var importedCount = 0
        
        try await database.write { db in
            var appTitleToIDMap: [String: UUID] = [:]
            
            if let specificAppID = appID {
                appTitleToIDMap["*"] = specificAppID
            } else {
                let appTitles = Set(dataFrame["appTitle", String.self].compactMap { $0 })

                for appTitle in appTitles {
                    let existingApp = try DBApp
                        .where { $0.title.eq(appTitle) }
                        .fetchOne(db)
                    
                    if let existingApp = existingApp {
                        appTitleToIDMap[appTitle] = existingApp.id
                    } else {
                        let newAppID = UUID()
                        try DBApp.insert {
                            DBApp.Draft(id: newAppID, title: appTitle)
                        }
                        .execute(db)
                        appTitleToIDMap[appTitle] = newAppID
                    }
                }
            }
            
            for rowIndex in 0..<dataFrame.rows.count {
                let finalAppID: UUID
                
                if let specificAppID = appID {
                    finalAppID = specificAppID
                } else {
                    guard
                        let appTitle = dataFrame["appTitle", String.self][rowIndex],
                        let mappedAppID = appTitleToIDMap[appTitle]
                    else {
                        reportIssue("Could not find app ID for row \(rowIndex)")
                        continue
                    }
                    finalAppID = mappedAppID
                }
                
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
                        id: UUID(),
                        date: date,
                        AppID: finalAppID,
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
