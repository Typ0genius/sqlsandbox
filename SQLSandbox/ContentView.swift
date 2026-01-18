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
internal import Combine

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
    
    let id = UUID()
    
    // Die Variable, die du haben möchtest
    var lastCount: Int?
    
    // Private State für die Pause-Logik
    private var pendingUpdate: Int?
    private var cancellables = Set<AnyCancellable>()
    
    init() {
        _samplesCount = FetchOne(SampleTable.count())
//        setupPublisherObserver()
    }
    
    private func setupPublisherObserver() {
        $samplesCount.publisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] newValue in
                guard let self else { return }
                print("ChildModel [\(id.uuidString.prefix(4))]: Publisher triggered with \(String(describing: newValue))")
                // Update wird in handleUpdate verarbeitet
                self.pendingUpdate = newValue
            }
            .store(in: &cancellables)
    }
    
    func setLastCount(_ newValue: Int?) {
        lastCount = newValue
        print("ChildModel [\(id.uuidString.prefix(4))]: lastCount updated to \(String(describing: newValue))")
    }
    
    func handleUpdate(isPaused: Bool) {
        guard let pending = pendingUpdate else { return }
        
        if isPaused {
            print("ChildModel [\(id.uuidString.prefix(4))]: PAUSED - keeping pending update: \(String(describing: pending))")
        } else {
            print("ChildModel [\(id.uuidString.prefix(4))]: Applying pending update: \(String(describing: pending))")
            setLastCount(pending)
            pendingUpdate = nil
        }
    }
}

struct ChildCountView: View {
    @State private var model = ChildModel()
    
    let shouldPauseFetch: Bool
    
    var body: some View {
        VStack {
            Text("Child View")
                .font(.headline)
            
            if let lastCount = model.lastCount {
                Text("Last: \(lastCount.formatted())")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
        .padding()
        .background(Color.blue.opacity(0.1))
        .cornerRadius(8)
        .onChange(of: shouldPauseFetch) { _, newValue in
            // Bei jeder Änderung prüfen ob pending update angewendet werden soll
            model.handleUpdate(isPaused: newValue)
        }
        .onAppear {
            // Initial check
            model.handleUpdate(isPaused: shouldPauseFetch)
        }
        .task(id: shouldPauseFetch) {
            print("CHILD [\(model.id.uuidString.prefix(4))]: .task(id:) called, shouldPauseFetch=\(shouldPauseFetch)")
                    
            // Wenn pausiert, einfach nichts tun - der alte Task wird durch .task(id:) automatisch gecancelt
            guard !shouldPauseFetch else {
                print("CHILD [\(model.id.uuidString.prefix(4))]: PAUSED - doing nothing")
                return
            }
                    
            print("CHILD [\(model.id.uuidString.prefix(4))]: STARTING fetch")
            print("CHILD [\(model.id.uuidString.prefix(4))]: load() started")
            // Direkt awaiten ohne Wrapper-Task - so kann .task(id:) es richtig canceln
            try? await model.$samplesCount.load(SampleTable.count()).task
            print("CHILD [\(model.id.uuidString.prefix(4))]: load() completed")
        }
    }
}
