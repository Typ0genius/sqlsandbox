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
                Text("Fetched Count: \(samplesCount.formatted())")
            }
            Toggle("Show childs", isOn: $showChilds)
                .toggleStyle(.switch)
                
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
    }
    
    private func performOldImport() async {
        let insertCount = 1_000_000

        isImporting = true
        resultMessage = "Importing..."
        
        // Task canceln
        countTask?.cancel()
        
        do {
            let dataManager = DataManager()
            
            let df = DataManager.generateTestDataFrame(count: insertCount)
            
            let start = CFAbsoluteTimeGetCurrent()
            try await dataManager.oldImportDataFrame(df)
            
            let duration = CFAbsoluteTimeGetCurrent() - start
            let rps = Int(Double(insertCount) / max(duration, 0.0001))
            let message = "[Perf] Imported: \(insertCount) rows in \(String(format: "%.2f", duration))s (\(rps) rows/s)"
            
            let actualCount = try await database.read { db in
                try SampleTable
                    .select { $0.id.count() }
                    .fetchOne(db) ?? 0
            }
            
            resultMessage = message + "\nTotal rows in DB: \(actualCount)"
            
            // Subscription neu starten
            countTask = Task {
                try? await $samplesCount.load(SampleTable.count()).task
            }
        } catch {
            resultMessage = "Error: \(error.localizedDescription)"
        }
        
        isImporting = false
    }
}
