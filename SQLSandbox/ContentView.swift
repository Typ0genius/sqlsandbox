//
//  ContentView.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import Dependencies
import SQLiteData
import SwiftUI
import TabularData

struct ContentView: View {
    @Dependency(\.defaultDatabase) private var database
    
    @State private var isImporting = false
    @State private var resultMessage = ""
    @State private var isObserving = true
    @State private var showChilds = false
    @State private var displayedCount: Int?
    
    private let insertCount = 1_000_000
    
    // OHNE Query = keine automatische Observation
    @FetchOne var samplesCount: Int?
    
    var body: some View {
        VStack(spacing: 20) {
            Text("SQL Sandbox Performance Test")
                .font(.title)
            if let displayedCount {
                Text("Sample Count in DB: \(displayedCount.formatted())")
            }
            Toggle("Show childs", isOn: $showChilds)
                .toggleStyle(.switch)
            
            Toggle("Observe sample count", isOn: $isObserving)
                .toggleStyle(.switch)
                
            Text("Insert Count: \(insertCount.formatted())")
                .font(.headline)
            
            Button(action: {
                Task {
                    await performImport()
                }
            }) {
                if isImporting {
                    ProgressView()
                        .controlSize(.small)
                } else {
                    Text("Run Batches Performance Test")
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(isImporting)
            
            Button(action: {
                Task {
                    await performOldImport()
                }
            }) {
                if isImporting {
                    ProgressView()
                        .controlSize(.small)
                } else {
                    Text("Run Each Line Performance Test")
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(isImporting)
            
            if !resultMessage.isEmpty {
                Text(resultMessage)
                    .font(.system(.body, design: .monospaced))
                    .padding()
                    .background(Color.secondary.opacity(0.1))
                    .cornerRadius(8)
            }

            if showChilds {
                HStack(spacing: 12) {
                    ChildCountView(title: "Child A", isObserving: isObserving)
                    ChildCountView(title: "Child B", isObserving: isObserving)
                    ChildCountView(title: "Child C", isObserving: isObserving)
                }
            }
        }
        .padding()
        .task(id: isObserving) {
            guard isObserving else {
                print("Parent observation skipped (isObserving: false)")
                return
            }
            print("Parent observation start")
            try? await $samplesCount.load(SampleTable.count()).task
            print("Parent observation finished")
        }
        .onChange(of: samplesCount) { newValue in
            let valueDescription = newValue.map { "\($0)" } ?? "nil"
            print("Parent samplesCount changed -> \(valueDescription)")
            if let newValue {
                displayedCount = newValue
            }
        }
    }
    
    private func performOldImport() async {
        isImporting = true
        resultMessage = "Importing..."
        
        do {
            let dataManager = DataManager()
            
            // Print database location
            let dbPath = database.path
            print("📁 Database location: \(dbPath)")
            
            // Generate test data
            let df = DataManager.generateTestDataFrame(count: insertCount)
            
            let start = CFAbsoluteTimeGetCurrent()
            try await dataManager.oldImportDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            let message = "[Perf] Imported: \(insertCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)"
            print(message)
            
            // Verify data was inserted
            let actualCount = try await database.read { db in
                try SampleTable
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            resultMessage = message + "\nTotal rows in DB: \(actualCount)\nDB: \(dbPath)"
            
        } catch {
            resultMessage = "Error: \(error.localizedDescription)"
            print("Import error: \(error)")
        }
        
        isImporting = false
    }
    
    private func performImport() async {
        isImporting = true
        resultMessage = "Importing..."
        
        do {
            let dataManager = DataManager()
            
            // Print database location
            let dbPath = database.path
            print("📁 Database location: \(dbPath)")
            
            // Generate test data
            let df = DataManager.generateTestDataFrame(count: insertCount)
            
            let start = CFAbsoluteTimeGetCurrent()
            try await dataManager.importDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            let message = "[Perf] Imported: \(insertCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)"
            print(message)
            
            // Verify data was inserted
            let actualCount = try await database.read { db in
                try SampleTable
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            resultMessage = message + "\nTotal rows in DB: \(actualCount)\nDB: \(dbPath)"
            
        } catch {
            resultMessage = "Error: \(error.localizedDescription)"
            print("Import error: \(error)")
        }
        
        isImporting = false
    }

}

private struct ChildCountView: View {
    let title: String
    let isObserving: Bool

    // OHNE Query = keine automatische Observation
    @FetchOne var samplesCount: Int?
    
    // Persistenter Cache für den Wert
    @State private var displayedCount: Int?

    var body: some View {
        VStack(spacing: 8) {
            Text(title)
                .font(.headline)

            if let displayedCount {
                Text("Samples: \(displayedCount.formatted())")
                    .font(.subheadline)
            } else {
                Text("Samples: —")
                    .font(.subheadline)
            }
        }
        .padding()
        .frame(maxWidth: .infinity)
        .background(Color.secondary.opacity(0.1))
        .cornerRadius(8)
        .id(title)
        .task(id: isObserving) {
            guard isObserving else {
                print("\(title) observation skipped (isObserving: false)")
                return
            }
            print("\(title) observation start")
            try? await $samplesCount.load(SampleTable.count()).task
            print("\(title) observation finished")
        }
        .onChange(of: samplesCount) { newValue in
            let valueDescription = newValue.map { "\($0)" } ?? "nil"
            print("\(title) samplesCount changed -> \(valueDescription)")
            // Update displayedCount only with real values
            if let newValue {
                displayedCount = newValue
            }
        }
    }
}

#Preview {
    ContentView()
}
