import Dependencies
import Foundation

actor ObservationPauseBroadcaster {
    private var continuation: AsyncStream<Bool>.Continuation?
    
    func stream() -> AsyncStream<Bool> {
        AsyncStream { continuation in
            self.continuation = continuation
        }
    }
    
    func send(_ isPaused: Bool) {
        continuation?.yield(isPaused)
    }
}

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
    static func live(broadcaster: ObservationPauseBroadcaster) -> Self {
        Self(
            pause: {
                await broadcaster.send(true)
            },
            resume: {
                await broadcaster.send(false)
            }
        )
    }
    
    static let noop = Self(
        pause: {},
        resume: {}
    )
}

extension DependencyValues {
    var observationPauseBroadcaster: ObservationPauseBroadcaster {
        get { self[ObservationPauseBroadcasterKey.self] }
        set { self[ObservationPauseBroadcasterKey.self] = newValue }
    }
    
    var observationPauseService: ObservationPauseService {
        get { self[ObservationPauseServiceKey.self] }
        set { self[ObservationPauseServiceKey.self] = newValue }
    }
    
    private enum ObservationPauseBroadcasterKey: DependencyKey {
        static let liveValue = ObservationPauseBroadcaster()
        static let testValue = ObservationPauseBroadcaster()
        static let previewValue = ObservationPauseBroadcaster()
    }
    
    private enum ObservationPauseServiceKey: DependencyKey {
        static var liveValue: ObservationPauseService {
            ObservationPauseService.live(broadcaster: ObservationPauseBroadcasterKey.liveValue)
        }
        static var testValue: ObservationPauseService {
            ObservationPauseService.live(broadcaster: ObservationPauseBroadcasterKey.testValue)
        }
        static var previewValue: ObservationPauseService {
            ObservationPauseService.live(broadcaster: ObservationPauseBroadcasterKey.previewValue)
        }
    }
}
