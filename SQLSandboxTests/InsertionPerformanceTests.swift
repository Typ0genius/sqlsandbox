import Testing
import Foundation
import Dependencies
import TabularData
import SQLiteData
@testable import SQLSandbox

struct InsertionPerformanceTests {
    @Test(arguments: [100_000])
    func discoveryEngagementInsertPerformance(insertCount: Int) async throws {
        let envCount = ProcessInfo.processInfo.environment["INSERT_COUNT"].flatMap(Int.init(_:))
        let insertCount = envCount ?? insertCount
        
        try await withDependencies {
            $0.defaultDatabase = try appDatabase()
            $0.context = .test
        } operation: {
            @Dependency(\.defaultDatabase) var database
            let dataManager = DataManager()
            
            // Create test app
            let appID = UUID()
            try await database.write { db in
                try DBApp.insert {
                    DBApp.Draft(id: appID, title: "PERF.TEST.APP")
                }
                .execute(db)
            }
            
            let startDate = Date().addingTimeInterval(-Double(insertCount))
            var dates: [Date] = []
            dates.reserveCapacity(insertCount)
            for i in 0..<insertCount {
                dates.append(Calendar.current.date(byAdding: .second, value: i, to: startDate)!)
            }
            
            var df = DataFrame()
            df.append(column: Column(name: "appTitle", contents: Array(repeating: "PERF.TEST.APP", count: insertCount)))
            df.append(column: Column(name: "date", contents: dates))
            df.append(column: Column(name: "event", contents: Array(repeating: "Impression", count: insertCount)))
            df.append(column: Column(name: "pageType", contents: Array(repeating: "Product Page", count: insertCount)))
            df.append(column: Column(name: "sourceType", contents: Array(repeating: "Browse", count: insertCount)))
            df.append(column: Column(name: "engagementType", contents: Array(repeating: "Tap", count: insertCount)))
            df.append(column: Column(name: "device", contents: Array(repeating: "iPhone", count: insertCount)))
            df.append(column: Column(name: "platformVersion", contents: Array(repeating: "iOS 18", count: insertCount)))
            df.append(column: Column(name: "territory", contents: Array(repeating: "US", count: insertCount)))
            df.append(column: Column(name: "count", contents: (0..<insertCount).map { _ in Int.random(in: 1...3) }))
            df.append(column: Column(name: "uniqueCount", contents: (0..<insertCount).map { _ in Int.random(in: 1...3) }))
            
            let start = CFAbsoluteTimeGetCurrent()
            let importedCount = try await dataManager.importDataFrame(df, appID: appID)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            print("[Perf] Imported: \(importedCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)")
            
            // Verify data was inserted
            let actualCount = try await database.read { db in
                try SampleTable
                    .where { $0.AppID.eq(appID) }
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            #expect(actualCount == insertCount)
            #expect(importedCount == insertCount)
            
            // Additional verification - check some sample data
            let sampleData = try await database.read { db in
                try SampleTable
                    .where { $0.AppID.eq(appID) }
                    .limit(5)
                    .fetchAll(db)
            }
            
            #expect(sampleData.count > 0)
            #expect(sampleData.first?.event == "Impression")
            #expect(sampleData.first?.device == "iPhone")
        }
    }
}
