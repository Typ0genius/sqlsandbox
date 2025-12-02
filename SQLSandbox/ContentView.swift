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
    @State private var samplesCountTask: Task<Void, Never>?
    @State private var showChilds = false
    
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
        .onAppear {
            startObservingSamplesIfNeeded()
        }
        .onChange(of: isObserving) { observing in
            print("Parent onChange isObserving -> \(observing)")
            if observing {
                startObservingSamplesIfNeeded()
            } else {
                stopObservingSamples()
            }
        }
        .onChange(of: samplesCount) { newValue in
            let valueDescription = newValue.map { "\($0)" } ?? "nil"
            print("Parent samplesCount changed -> \(valueDescription)")
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

    private func startObservingSamplesIfNeeded() {
        guard samplesCountTask == nil, isObserving else {
            print("Parent observe skipped (task exists? \(samplesCountTask != nil), isObserving: \(isObserving))")
            return
        }
        print("Parent observe start (isObserving: \(isObserving))")
        samplesCountTask = Task {
            print("Parent fetch start")
            defer { print("Parent fetch finished") }
            try? await $samplesCount.load(SampleTable.count()).task
        }
    }

    private func stopObservingSamples() {
        print("Parent observe stop (existing task: \(samplesCountTask != nil))")
        if let samplesCountTask {
            samplesCountTask.cancel()
            print("Parent task cancelled")
        }
        samplesCountTask = nil
    }
}

private struct ChildCountView: View {
    let title: String
    let isObserving: Bool
    @State private var observationTask: Task<Void, Never>?

    @FetchOne(SampleTable.count())
    var childSamplesCount

    var body: some View {
        VStack(spacing: 8) {
            Text(title)
                .font(.headline)

            if let childSamplesCount {
                Text("Samples: \(childSamplesCount.formatted())")
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
        .onAppear {
            startObserving()
        }
        .onChange(of: isObserving) { observing in
            print("\(title) onChange isObserving -> \(observing)")
            if observing {
                startObserving()
            } else {
                stopObserving()
            }
        }
        .onChange(of: childSamplesCount) { newValue in
            let valueDescription = newValue.map { "\($0)" } ?? "nil"
            print("\(title) samplesCount changed -> \(valueDescription)")
        }
    }

    private func startObserving() {
        guard observationTask == nil, isObserving else {
            print("\(title) observe skipped (task exists? \(observationTask != nil), isObserving: \(isObserving))")
            return
        }
        print("\(title) observe start (isObserving: \(isObserving))")
        observationTask = Task {
            print("\(title) fetch start")
            defer { print("\(title) fetch finished") }
            try? await $childSamplesCount.load(SampleTable.count()).task
        }
    }

    private func stopObserving() {
        print("\(title) observe stop (existing task: \(observationTask != nil))")
        if let observationTask {
            observationTask.cancel()
            print("\(title) task cancelled")
        }
        observationTask = nil
    }
}

#Preview {
    ContentView()
}
