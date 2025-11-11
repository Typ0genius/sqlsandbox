import Dependencies
import Foundation
import SQLiteData
@testable import SQLSandbox
import TabularData
import Testing

struct InsertionPerformanceTests {
    @Test(arguments: [1_000_000])
    func oldInsertPerformance(insertCount: Int) async throws {
        let envCount = ProcessInfo.processInfo.environment["INSERT_COUNT"].flatMap(Int.init(_:))
        let insertCount = envCount ?? insertCount
        
        try await withDependencies {
            $0.defaultDatabase = try appDatabase()
            $0.context = .test
        } operation: {
            @Dependency(\.defaultDatabase) var database
            let dataManager = DataManager()
            
            // Generate test data using shared function
            let df = DataManager.generateTestDataFrame(count: insertCount)
            
            let start = CFAbsoluteTimeGetCurrent()
            let importedCount = try await dataManager.oldImportDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            print("[Perf] Imported: \(importedCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)")
            
            let actualCount = try await database.read { db in
                try SampleTable
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            #expect(actualCount == insertCount)
            #expect(importedCount == insertCount)
        }
    }
    
    @Test(arguments: [1_000_000])
    func newInsertPerformance(insertCount: Int) async throws {
        let envCount = ProcessInfo.processInfo.environment["INSERT_COUNT"].flatMap(Int.init(_:))
        let insertCount = envCount ?? insertCount
        
        try await withDependencies {
            $0.defaultDatabase = try appDatabase()
            $0.context = .test
        } operation: {
            @Dependency(\.defaultDatabase) var database
            let dataManager = DataManager()
            
            // Generate test data using shared function
            let df = DataManager.generateTestDataFrame(count: insertCount)
            
            let start = CFAbsoluteTimeGetCurrent()
            let importedCount = try await dataManager.importDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            print("[Perf] Imported: \(importedCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)")
            
            let actualCount = try await database.read { db in
                try SampleTable
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            #expect(actualCount == insertCount)
            #expect(importedCount == insertCount)
        }
    }
}
