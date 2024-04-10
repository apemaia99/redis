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
    fileprivate var configurations: [RedisID: RedisConfiguration]
    fileprivate var pools: [PoolKey: [Pool]]

    struct Pool {
        let pool: RedisConnectionPool
        let role: RedisRole
    }

    init(app: Application) {
        application = app
        lock = .init()
        configurations = [:]
        pools = [:]
    }

    func use(_ redisConfiguration: RedisConfiguration, as id: RedisID = .default) {
        lock.lock()
        defer { lock.unlock() }

        let newConfiguration = redisConfiguration
        let currentConfiguration = configurations[id]

        switch (newConfiguration, currentConfiguration) {
        case (let .highAvailability(sentinel: _, redis: newConfigurations), .highAvailability):
            application.logger.trace("DISCOVERED NETWORK: \(newConfigurations)")
            shutdown(for: id, roles: [.master, .slave])
            for nodeConfiguration in newConfigurations {
                bootstrap(for: id, using: nodeConfiguration)
            }
        case (let .highAvailability(sentinel: sentinelConfiguration, redis: _), .none):
            application.logger.trace("FIRST BOOT, we must discover network: \(sentinelConfiguration)")
            bootstrap(for: id, using: sentinelConfiguration)
        case let (.standalone(configuration), .none):
            bootstrap(for: id, using: configuration)
        default:
            fatalError("OUT OF CONTEXT")
        }

        configurations[id] = newConfiguration
    }

    func configuration(for id: RedisID = .default) -> RedisConfiguration? {
        configurations[id]
    }

    func ids() -> Set<RedisID> {
        Set(configurations.keys)
    }

    func pool(for eventLoop: EventLoop, id redisID: RedisID, role: RedisRole) -> RedisConnectionPool {
        let key = PoolKey(eventLoopKey: eventLoop.key, redisID: redisID)
        guard let pools = pools[key],
              let pool = pools.first(where: { $0.role == role })?.pool
        else {
            fatalError("No redis found for id \(redisID), or the app may not have finished booting. Also, the eventLoop must be from Application's EventLoopGroup.")
        }
        return pool
    }

    private func monitor(id: RedisID) -> EventLoopFuture<Void> {
        let sentinel = pool(for: application.eventLoopGroup.next(), id: id, role: .sentinel)
        return sentinel.psubscribe(to: "*") { key, message in
            switch key {
            case "+switch-master":
                self.application.logger.notice("NEW MASTER: \(message)")
                self.discovery(id: id).map { newConfiguration in
                    self.use(newConfiguration, as: id)
                    self.application.logger.notice("END DISCOVERY AFTER SWITCH MASTER")
                }

            default:
                self.application.logger.notice("CHANNEL: \(key) | \(message)")
            }
        } onSubscribe: { subscriptionKey, _ in
            self.application.logger.trace("SUB TO \(subscriptionKey)")
        } onUnsubscribe: { subscriptionKey, _ in
            self.application.logger.trace("UNSUB TO \(subscriptionKey)")
        }
    }

    func discovery(id: RedisID) -> EventLoopFuture<RedisConfiguration> {
        application.logger.notice("START DISCOVERY")
        let sentinel = pool(for: application.eventLoopGroup.next(), id: id, role: .sentinel)

        let configuration = self.configuration(for: id)!
        let discover = RedisTopologyDiscover(sentinel: sentinel, configuration: configuration, logger: application.logger)
        return discover.discovery(for: id)
    }
}

extension RedisStorage {
    private func bootstrap(for id: RedisID, using redisConfiguration: RedisConfiguration.Configuration) {
        let newPool = makePools(for: id, using: redisConfiguration)
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

            guard let idx = pools[key]?.firstIndex(where: { roles.contains($0.role) }),
                  let pool = pools[key]?.remove(at: idx).pool
            else { continue }
            application.logger.trace("SHUTTING DOWN: \(pool)")
            pool.close(promise: nil, logger: application.logger)
        }
    }

    private func makePools(for id: RedisID, using configuration: RedisConfiguration.Configuration) -> [PoolKey: [Pool]] {
        var newPools: [PoolKey: [Pool]] = [:]

        for eventLoop in application.eventLoopGroup.makeIterator() {
            let newKey: PoolKey = PoolKey(eventLoopKey: eventLoop.key, redisID: id)
            let newPool: RedisConnectionPool = makePool(using: configuration, on: eventLoop, logger: application.logger)

            newPools[newKey] = [.init(pool: newPool, role: configuration.role)]
        }

        return newPools
    }

    private func makePool(using configuration: RedisConfiguration.Configuration, on eventLoop: EventLoop, logger: Logger) -> RedisConnectionPool {
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
                case .highAvailability:
                    application.logger.trace("START BOOT DISCOVERY FOR: \(id)")
                    redisStorage.discovery(id: id).whenSuccess { newConfiguration in
                        application.logger.notice("UPDATING...")
                        self.redisStorage.use(newConfiguration, as: id)
                        self.redisStorage.monitor(id: id).whenSuccess { _ in
                            application.logger.notice("ATTACHED TO PUBSUB")
                        }
                    }

                case .standalone:
                    break // NO FURTHER ACTIONS
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
