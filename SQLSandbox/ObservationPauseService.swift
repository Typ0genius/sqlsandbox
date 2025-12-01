import Dependencies
import Foundation

struct ObservationPauseService: Sendable {
    var pause: @Sendable () async -> Void
    var resume: @Sendable () async -> Void
    
    func withPausedObservations<T>(
        _ operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        await pause()
        do {
            let result = try await operation()
            await resume()
            return result
        } catch {
            await resume()
            throw error
        }
    }
}

extension ObservationPauseService {
    static let live = Self(
        pause: {
            await MainActor.run {
                NotificationCenter.default.post(name: .observationsPaused, object: nil)
            }
        },
        resume: {
            await MainActor.run {
                NotificationCenter.default.post(name: .observationsResumed, object: nil)
            }
        }
    )
    
    static let noop = Self(
        pause: {},
        resume: {}
    )
}

extension DependencyValues {
    var observationPauseService: ObservationPauseService {
        get { self[ObservationPauseServiceKey.self] }
        set { self[ObservationPauseServiceKey.self] = newValue }
    }
    
    private enum ObservationPauseServiceKey: DependencyKey {
        static let liveValue = ObservationPauseService.live
        static let testValue = ObservationPauseService.noop
        static let previewValue = ObservationPauseService.noop
    }
}

extension Notification.Name {
    static let observationsPaused = Notification.Name("ObservationPauseService.observationsPaused")
    static let observationsResumed = Notification.Name("ObservationPauseService.observationsResumed")
}
