//
//  SQLSandboxApp.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import SwiftUI
import SQLiteData
import Dependencies

@main
struct SQLSandboxApp: App {
    init() {
        prepareDependencies {
            $0.defaultDatabase = try! appDatabase()
            $0.context = .live
        }
    }
    
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
