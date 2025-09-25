//
//  SQLSandboxApp.swift
//  SQLSandbox
//
//  Created by Typ0genius on 25/9/25.
//

import SwiftUI
import SQLiteData

@main
struct SQLSandboxApp: App {
    init() {
        prepareDependencies {
          let db = try! DatabaseQueue(
            // Create/migrate a database
            // connection
          )
          $0.defaultDatabase = db
        }
      }
    
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
