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
    private var configurations: [RedisID: RedisMode] = [:]
    private var monitoring: NIOLockedValueBox<[RedisID: Bool]>
    private var pools: [PoolKey: [Pool]]

    struct Pool {
        let pool: RedisConnectionPool
        let role: RedisRole
    }

    init(app: Application) {
        application = app
        lock = .init()
        configurations = [:]
        monitoring = .init([:])
        pools = [:]
    }

    func use(_ mode: RedisMode, as id: RedisID = .default) {
        lock.lock()
        defer { lock.unlock() }

        configurations[id] = mode

        switch mode {
        case let .standalone(configuration):
            bootstrap(for: id, using: configuration)
        case let .highAvailability(sentinel, _):
            bootstrap(for: id, using: sentinel.configuration)
        }
    }

    func configuration(for id: RedisID = .default) -> RedisMode? {
        lock.withLock { configurations[id] }
    }

    func ids() -> Set<RedisID> {
        lock.withLock { Set(configurations.keys) }
    }

    func pool(for eventLoop: EventLoop, id redisID: RedisID, role: RedisRole) -> RedisConnectionPool {
        lock.lock()
        defer { lock.unlock() }

        application.logger.notice("REQUEST CLIENT FOR ID: \(redisID), ROLE: \(role)")
        let key = PoolKey(eventLoopKey: eventLoop.key, redisID: redisID)

        guard let pools = pools[key],
              let client = pools.first(where: { $0.role == role }),
              let configuration = configurations[redisID]
        else {
            fatalError("No redis found for id \(redisID), or the app may not have finished booting. Also, the eventLoop must be from Application's EventLoopGroup.")
        }

        if case let .highAvailability(_, redis) = configuration, client.role != .sentinel {
            monitor(eventLoop: eventLoop, id: redisID, configuration: redis.configuration)
        }

        return client.pool
    }

    fileprivate func discovery(id: RedisID, configuration: RedisMode.Configuration) -> EventLoopFuture<Void> {
        application.logger.notice("START DISCOVERY")
        let sentinel = pool(for: application.eventLoopGroup.next(), id: id, role: .sentinel)
        let discover = RedisTopologyDiscover(sentinel: sentinel, configuration: configuration, logger: application.logger)
        return discover.discovery(for: id).map {
            self.refresh(nodes: $0, for: id)
        }
    }
}

extension RedisStorage {
    private func bootstrap(for id: RedisID, using configuration: RedisMode.Configuration) {
        let newPool = makePools(for: id, using: configuration)

        for (key, pool) in newPool {
            if pools[key] != nil {
                application.logger.trace("RUNTIME UPDATE FOR: \(key)")
                pools[key]?.append(contentsOf: pool)
            } else {
                application.logger.trace("BOOTSTRAP: \(pool)")
                pools[key] = pool
            }
        }
    }

    private func shutdown(for id: RedisID, roles: Set<RedisRole>) {
        for eventLoop in application.eventLoopGroup.makeIterator() {
            let key = PoolKey(eventLoopKey: eventLoop.key, redisID: id)

            guard let pool = pools[key] else { continue }
            pools[key]?.removeAll(where: { roles.contains($0.role) })

            pool.filter({ roles.contains($0.role) })
                .map(\.pool)
                .forEach { pool in
                    application.logger.trace("SHUTTING DOWN: \(pool)")
                    pool.close(promise: nil, logger: application.logger)
                }
        }
    }

    private func refresh(nodes: [RedisNode], for id: RedisID) {
        lock.lock()
        defer { lock.unlock() }
        
        let nodesConfigurations = nodes.map(\.configuration)
        shutdown(for: id, roles: [.master, .slave])
        for nodeConfiguration in nodesConfigurations {
            bootstrap(for: id, using: nodeConfiguration)
        }
    }

    private func monitor(eventLoop: EventLoop, id: RedisID, configuration: RedisMode.Configuration) -> EventLoopFuture<Void> {
        application.logger.notice("REQUESTED CLIENT FOR HA, CHECK IF MONITORED")

        let monitored = monitoring.withLockedValue { monitors in
            let monitored = monitors[id] ?? true
            if !monitored { monitors[id] = true } // ACT AS LOCK DURING SUBSCRIPTION
            return monitored
        }

        guard !monitored else {
            application.logger.notice("MONITOR ALREADY ACTIVE, NOTHING TO DO")
            return eventLoop.makeSucceededVoidFuture()
        }

        application.logger.notice("NOT ATTACHED, TRY MONITORING")
        let sentinel = pool(for: eventLoop, id: id, role: .sentinel)
        return sentinel.psubscribe(to: "*") { key, message in
            switch key {
            case "+switch-master":
                self.application.logger.notice("NEW MASTER: \(message)")
                self.discovery(id: id, configuration: configuration).map {
                    self.application.logger.notice("END DISCOVERY AFTER SWITCH MASTER")
                }
            default:
                self.application.logger.notice("CHANNEL: \(key) | \(message)")
            }
        } onSubscribe: { subscriptionKey, _ in
            self.application.logger.trace("SUBSCRIBED TO \(subscriptionKey)")
            self.monitoring.withLockedValue({ $0[id] = true })
        } onUnsubscribe: { subscriptionKey, _ in
            self.application.logger.trace("UNSUBSCRIBED FROM \(subscriptionKey)")
            self.monitoring.withLockedValue({ $0[id] = false })
        }.recover { error in
            self.application.logger.trace("SUBSCRIBE FAILED DUE TO: \(error)")
            self.monitoring.withLockedValue({ $0[id] = false })
        }
    }

    private func makePools(for id: RedisID, using configuration: RedisMode.Configuration) -> [PoolKey: [Pool]] {
        var newPools: [PoolKey: [Pool]] = [:]

        for eventLoop in application.eventLoopGroup.makeIterator() {
            let newKey: PoolKey = PoolKey(eventLoopKey: eventLoop.key, redisID: id)
            let newPool: RedisConnectionPool = makePool(using: configuration, on: eventLoop, logger: application.logger)

            newPools[newKey] = [.init(pool: newPool, role: configuration.role)]
        }

        return newPools
    }

    private func makePool(using configuration: RedisMode.Configuration, on eventLoop: EventLoop, logger: Logger) -> RedisConnectionPool {
        let redisTLSClient: ClientBootstrap? = {
            guard let tlsConfig = configuration.tlsConfiguration,
                  let tlsHost = configuration.tlsHostname else { return nil }

            return ClientBootstrap(group: eventLoop)
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                .channelInitializer { channel in
                    do {
                        let sslContext = try NIOSSLContext(configuration: tlsConfig)
                        return EventLoopFuture.andAllSucceed([
                            channel.pipeline.addHandler(try NIOSSLClientHandler(context: sslContext,
                                                                                serverHostname: tlsHost)),
                            channel.pipeline.addBaseRedisHandlers(),
                        ], on: channel.eventLoop)
                    } catch {
                        return channel.eventLoop.makeFailedFuture(error)
                    }
                }
        }()

        logger.notice("CREATION OF POOL CONF IP:\(configuration.serverAddresses)")

        let newPool = RedisConnectionPool(
            configuration: .init(configuration, defaultLogger: logger, customClient: redisTLSClient),
            boundEventLoop: eventLoop
        )

        return newPool
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

        func didBoot(_ application: Application) throws {
            for (id, configuration) in redisStorage.configurations {
                switch configuration {
                case .standalone:
                    break // Nothing to do
                case let .highAvailability(sentinel: _, redis: redis):
                    application.logger.trace("START BOOT DISCOVERY FOR: \(id)")
                    let newConfiguration = try redisStorage.discovery(id: id, configuration: redis.configuration).wait()
                    try redisStorage.monitor(eventLoop: application.eventLoopGroup.next(), id: id, configuration: redis.configuration).wait()
                    application.logger.trace("SUBSCRIBED")
                }
            }
        }

        /// Close each connection pool
        func shutdown(_ application: Application) {
            redisStorage.lock.lock()
            defer { self.redisStorage.lock.unlock() }

            let shutdownFuture: EventLoopFuture<Void> = redisStorage.pools
                .flatMap(\.value)
                .map(\.pool)
                .map { pool in
                    let promise = pool.eventLoop.makePromise(of: Void.self)
                    pool.close(promise: promise)
                    return promise.futureResult
                }.flatten(on: application.eventLoopGroup.next())

            do {
                try shutdownFuture.wait()
            } catch {
                application.logger.error("Error shutting down redis connection pools, possibly because the pool never connected to the Redis server: \(error)")
            }
        }
    }
}

private extension RedisStorage {
    /// Since a `RedisConnectionPool` is created for each `RedisID` on each `EventLoop`, combining
    /// the `RedisID` and the `EventLoop` into one key simplifies the storage dictionary
    struct PoolKey: Hashable, StorageKey {
        typealias Value = RedisConnectionPool

        let eventLoopKey: EventLoop.Key
        let redisID: RedisID
    }
}

private extension EventLoop {
    typealias Key = ObjectIdentifier
    var key: Key {
        ObjectIdentifier(self)
    }
}
