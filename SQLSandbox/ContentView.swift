//
//  ContentView.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import SwiftUI
import Dependencies
import SQLiteData
import TabularData

struct ContentView: View {
    @Dependency(\.defaultDatabase) private var database
    
    @State private var isImporting = false
    @State private var resultMessage = ""
    
    private let insertCount = 1_000_000
    
    var body: some View {
        VStack(spacing: 20) {
            Text("SQL Sandbox Performance Test")
                .font(.title)
            
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
                    Text("Run Performance Test")
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
