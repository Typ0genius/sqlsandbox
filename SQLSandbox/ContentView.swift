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
    @State private var showChilds = false
    
    @FetchOne var samplesCount: Int?
    @State var countTask: Task<Void, Never>?
    
    var body: some View {
        VStack(spacing: 20) {
            Text("SQL Sandbox Performance Test")
                .font(.title)
            if let samplesCount {
                Text("Fetched Count (Parent): \(samplesCount.formatted())")
            }
            Toggle("Show childs", isOn: $showChilds)
                .toggleStyle(.switch)
            
            if showChilds {
                HStack(spacing: 20) {
                    ChildCountView(shouldPauseFetch: isImporting)
                    ChildCountView(shouldPauseFetch: isImporting)
                }
            }
                
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
            countTask = Task {
                try? await $samplesCount.load(SampleTable.count()).task
            }
        }
        .onChange(of: showChilds) { oldValue, newValue in
            print("=== TOGGLE: showChilds changed \(oldValue) -> \(newValue) ===")
        }
    }
    
    private func performOldImport() async {
        let insertCount = 1_000_000

        print("=== IMPORT STARTED ===")
        isImporting = true
        resultMessage = "Importing..."
        
        // Cancel Task
        countTask?.cancel()
        
        do {
            let dataManager = DataManager()
            
            let df = DataManager.generateTestDataFrame(count: insertCount)
            
            let start = CFAbsoluteTimeGetCurrent()
            try await dataManager.oldImportDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            let message = "[Perf] Imported: \(insertCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)"
            
            print("=== IMPORT FINISHED: \(String(format: "%.2f", duration))s (\(rps) rows/s) ===")
            
            let actualCount = try await database.read { db in
                try SampleTable
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            resultMessage = message + "\nTotal rows in DB: \(actualCount)"
            
            // Subscribe again
            countTask = Task {
                try? await $samplesCount.load(SampleTable.count()).task
            }
        } catch {
            resultMessage = "Error: \(error.localizedDescription)"
        }
        
        isImporting = false
    }
}

// MARK: - Child Model (eigenes Model pro Child)

@Observable
class ChildModel {
    @ObservationIgnored
    @FetchOne var samplesCount: Int?
    
    @ObservationIgnored
    private var fetchTask: Task<Void, Never>?
    
    let id = UUID()
    
    func pauseChanged(newValue: Bool) {
        if newValue {
            // Cancel observation
            print("CHILD [\(id.uuidString.prefix(4))]: Cancelling fetch task")
            fetchTask?.cancel()
            fetchTask = nil
        } else {
            // Start/continue observation
            print("CHILD [\(id.uuidString.prefix(4))]: Starting fetch task")
            startFetching()
        }
    }
    
    func startFetching() {
        // Cancel existing task if any
        fetchTask?.cancel()
        
        fetchTask = Task {
            print("CHILD [\(id.uuidString.prefix(4))]: STARTING fetch")
            print("CHILD [\(id.uuidString.prefix(4))]: load() started")
            
            guard !Task.isCancelled else {
                print("CHILD [\(id.uuidString.prefix(4))]: Task was cancelled before starting")
                return
            }
            
            try? await $samplesCount.load(SampleTable.count()).task
            
            guard !Task.isCancelled else {
                print("CHILD [\(id.uuidString.prefix(4))]: Task was cancelled after load")
                return
            }
            
            print("CHILD [\(id.uuidString.prefix(4))]: load() completed")
            fetchTask = nil
        }
    }
    
    func stopFetching() {
        print("CHILD [\(id.uuidString.prefix(4))]: Stopping fetch task")
        fetchTask?.cancel()
        fetchTask = nil
    }
}

struct ChildCountView: View {
    @State private var model = ChildModel()
    
    let shouldPauseFetch: Bool
    
    var body: some View {
        VStack {
            Text("Child View")
                .font(.headline)
            Text("Last Count: \(model.samplesCount ?? -1)")
        }
        .padding()
        .background(Color.blue.opacity(0.1))
        .cornerRadius(8)
        .onAppear {
            model.startFetching()
        }
        .onDisappear {
            model.stopFetching()
        }
        .onChange(of: shouldPauseFetch) { _, newValue in
            model.pauseChanged(newValue: newValue)
        }
        .onChange(of: model.samplesCount) {
            if shouldPauseFetch {
                model.pauseChanged(newValue: true)
            }
        }
        .onChange(of: model.samplesCount) {
            print("DISPLAYED DATA UPDATE TRIGGERED \(model.id) \(model.samplesCount ?? -1)")
        }
    }
}
