import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL
import RediStack
import Vapor

extension Application {
    private struct RedisStorageKey: StorageKey {
        typealias Value = RedisStorage
    }

    var redisStorage: RedisStorage {
        if let existing = storage[RedisStorageKey.self] {
            return existing
        }

        let redisStorage = RedisStorage(app: self)
        storage[RedisStorageKey.self] = redisStorage
        lifecycle.use(RedisStorage.Lifecycle(redisStorage: redisStorage))
        return redisStorage
    }
}

final class RedisStorage {
    private let application: Application
    private var lock: NIOLock
    private var clusters: [RedisID: RedisCluster]

    init(app: Application) {
        application = app
        lock = .init()
        clusters = [:]
    }

    func use(_ mode: RedisMode, as id: RedisID = .default) {
        lock.lock()
        defer { lock.unlock() }

        let commonMetadata = Logger.MetadataProvider({ ["id": .stringConvertible(id)] })
        var monitoringLogger = Logger(label: "vapor.redis.cluster.monitoring", metadataProvider: commonMetadata)
        var clusterLogger = Logger(label: "vapor.redis.cluster", metadataProvider: commonMetadata)
        monitoringLogger.logLevel = application.logger.logLevel
        clusterLogger.logLevel = application.logger.logLevel

        let monitoring = RedisClusterMonitor(logger: monitoringLogger)

        clusters[id] = .init(application: application, mode: mode, logger: clusterLogger, monitoring: monitoring)
    }

    func configuration(for id: RedisID = .default) -> RedisMode? {
        lock.withLock { clusters[id]?.mode }
    }

    func ids() -> Set<RedisID> {
        lock.withLock { Set(clusters.keys) }
    }

    func pool(for eventLoop: EventLoop, id redisID: RedisID, role: RedisRole) -> RedisConnectionPool {
        lock.lock()
        defer { lock.unlock() }

        guard let cluster = clusters[redisID] else { fatalError("Instance of \(redisID) not configured") }

        return cluster.pool(for: eventLoop, role: role)
    }
}

extension RedisStorage {
    /// Lifecyle Handler for Redis Storage. On boot, it creates a RedisConnectionPool for each
    /// configurated `RedisID` on each `EventLoop`.
    final class Lifecycle: LifecycleHandler {
        unowned let redisStorage: RedisStorage
        init(redisStorage: RedisStorage) {
            self.redisStorage = redisStorage
        }

        func willBoot(_ application: Application) throws {
            redisStorage.clusters.values.forEach({ $0.bootstrap() })
        }

        func didBoot(_ application: Application) throws {
            try redisStorage.clusters.filter { _, cluster in
                switch cluster.mode {
                case .highAvailability:
                    return true
                case .standalone:
                    return false
                }
            }.forEach { _, cluster in
                try cluster.discovery(for: application.eventLoopGroup.next()).wait()
            }
        }

        /// Close each connection pool
        func shutdown(_ application: Application) {
            redisStorage.lock.lock()
            defer { self.redisStorage.lock.unlock() }

            let eventLoop = application.eventLoopGroup.next()
            let shutdownFuture = redisStorage.clusters.values
                .map({ $0.shutdown(on: eventLoop) })
                .flatten(on: eventLoop)

            do {
                try shutdownFuture.wait()
            } catch {
                application.logger.error("Error shutting down redis connection pools, possibly because the pool never connected to the Redis server: \(error)")
            }
        }
    }
}
