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
    @Dependency(\.observationPauseBroadcaster) private var pauseBroadcaster
    
    @State private var isImporting = false
    @State private var resultMessage = ""
    @State private var isObserving = true
    @State private var isExternallyPaused = false
    @State private var observationTask: Task<Void, any Error>?
    
    private let insertCount = 1_000_000
    
    @FetchOne(SampleTable.count())
    var samplesCount
    
    var body: some View {
        VStack(spacing: 20) {
            Text("SQL Sandbox Performance Test")
                .font(.title)
            if let samplesCount {
                Text("Sample Count in DB: \(samplesCount.formatted())")
            }
            
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
        }
        .padding()
        .task {
            for await isPaused in await pauseBroadcaster.stream() {
                isExternallyPaused = isPaused
                if isPaused {
                    observationTask?.cancel()
                    observationTask = nil
                }
            }
        }
        .task(id: isObserving && !isExternallyPaused) {
            guard isObserving && !isExternallyPaused else {
                observationTask?.cancel()
                observationTask = nil
                return
            }
            do {
                let fetch = try await $samplesCount.load(SampleTable.count())
                observationTask = Task {
                    try await fetch.task
                }
            } catch {
                // In a demo app we just log the error and stop observing.
                print("Observation error: \(error)")
                observationTask = nil
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
            let importedCount = try await dataManager.oldImportDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            let message = "[Perf] Imported: \(importedCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)"
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
            let importedCount = try await dataManager.importDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            let message = "[Perf] Imported: \(importedCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)"
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

#Preview {
    ContentView()
}
