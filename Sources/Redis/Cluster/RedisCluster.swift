import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL
import RediStack
import Vapor

final class RedisCluster {
    fileprivate typealias PoolKey = EventLoop.Key

    let mode: RedisMode

    private var master: [PoolKey: RedisConnectionPool]
    private var sentinel: [PoolKey: RedisConnectionPool]
    private var replicas: [RedisClusterNodeID: [PoolKey: RedisConnectionPool]]

    private let application: Application
    private let logger: Logger
    private let monitoring: RedisClusterMonitorProviding
    private let lock: NIOLock

    init(application: Application, mode: RedisMode, logger: Logger, monitoring: RedisClusterMonitorProviding) {
        self.mode = mode
        self.application = application
        self.logger = logger
        self.monitoring = monitoring
        master = [:]
        sentinel = [:]
        replicas = [:]
        lock = .init()
        self.monitoring.delegate = self
    }

    func pool(for eventLoop: EventLoop, role: RedisRole) -> RedisConnectionPool {
        lock.lock()
        defer { lock.unlock() }

        let key = eventLoop.key
        let connection: RedisConnectionPool?

        logger.debug("picking client from pool", metadata: ["role": .stringConvertible(role), "eventLoop": .string(key.debugDescription)])

        switch role {
        case .master:
            connection = master[key]
        case .slave:
            connection = replicas.values.randomElement()?[key]
        case .sentinel:
            connection = sentinel[key]
        }

        guard let connection else {
            return master[key]! // At last resort we return the master that is always present.
        }

        if case .highAvailability = mode, role != .sentinel {
            if let sentinel = sentinel[eventLoop.key] {
                logger.trace("trigger cluster monitor", metadata: ["sentinel": .stringConvertible(sentinel.id)])
                monitoring.start(using: sentinel)
            }
        }

        return connection
    }

    func discovery(for eventLoop: EventLoop) -> EventLoopFuture<Void> {
        logger.trace("start cluster discovery")

        guard
            case let .highAvailability(sentinelConfiguration, _) = mode,
            let name = sentinelConfiguration.configuration.masterName
        else {
            return eventLoop.makeFailedFuture(NSError(domain: "UNSUPPORTED WHNE NOT IN HA", code: -1))
        }

        let key = eventLoop.key
        guard let sentinel = sentinel[key] else {
            return eventLoop.makeFailedFuture(NSError(domain: "MISSING SENTINEL", code: -1))
        }

        let discover = RedisTopologyDiscover(sentinel: sentinel, masterName: name, logger: logger)

        return discover.discovery()
            .flatMapThrowing { nodes in
                self.logger.trace("end of cluster discovery")
                for node in nodes {
                    try self.refresh(of: node)
                }
                self.logger.notice("updated topology")
                self.monitoring.start(using: sentinel)
            }
    }

    func bootstrap() {
        logger.trace("requested bootstrap of redis")
        switch mode {
        case let .standalone(configuration):
            logger.debug("standalone mode")
            let newPool = makePools(using: configuration)
            master = newPool
        case let .highAvailability(sentinel, _):
            logger.debug("highAvailability mode")
            let newPool = makePools(using: sentinel.configuration)
            self.sentinel = newPool
        }
    }

    func shutdown(on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        logger.trace("requested shutdown of redis")

        let master = master.values
        let sentinel = sentinel.values
        let replicas = replicas.values.flatMap(\.values)

        self.master.removeAll()
        self.sentinel.removeAll()
        self.replicas.removeAll()

        let masterShutdownFutures = master.map {
            let promise = $0.eventLoop.makePromise(of: Void.self)
            $0.close(promise: promise, logger: logger)
            return promise.futureResult
        }.flatten(on: eventLoop)

        let sentinelShutdownFutures = sentinel.map {
            let promise = $0.eventLoop.makePromise(of: Void.self)
            $0.close(promise: promise, logger: logger)
            return promise.futureResult
        }.flatten(on: eventLoop)

        let replicaShutdownFutures = replicas.map {
            let promise = $0.eventLoop.makePromise(of: Void.self)
            $0.close(promise: promise, logger: logger)
            return promise.futureResult
        }.flatten(on: eventLoop)

        return [masterShutdownFutures, sentinelShutdownFutures, replicaShutdownFutures]
            .flatten(on: eventLoop)
    }
}

extension RedisCluster: RedisClusterMonitoringDelegate {
    func monitoring(changed status: MonitoringStatus) {
        logger.info("cluster monitor status change", metadata: ["status": .stringConvertible(status)])

        guard status == .dropped, let sentinel = sentinel[application.eventLoopGroup.next().key] else { return }

        logger.info("monitor lost, trying another sentinel")
        monitoring.start(using: sentinel)
    }

    func monitoring(shouldRefreshTopology: Bool) {
        logger.info("Monitoring suggest to refresh topology: \(shouldRefreshTopology)")
        guard shouldRefreshTopology else { return }
        discovery(for: application.eventLoopGroup.next())
    }

    func switchMaster(from oldMaster: RediStack.RedisClusterNodeID, to newMaster: RediStack.RedisClusterNodeID) {
        logger.notice(
            "master changed",
            metadata: [
                "Old master": .string(.init(describing: oldMaster)),
                "New master": .string(.init(describing: newMaster)),
            ]
        )

        do {
            try refresh(of: .init(from: newMaster, role: .master))
        } catch {
            logger.error("Cannot refresh master node due to: \(error)")
        }
    }

    func detected(replica: RediStack.RedisClusterNodeID, relatedTo master: RediStack.RedisClusterNodeID) {
        logger.notice(
            "replica detected",
            metadata: [
                "replica": .string(.init(describing: replica)),
                "master": .string(.init(describing: master)),
            ])

        do {
            try refresh(of: .init(from: replica, role: .slave))
        } catch {
            logger.error("Cannot refresh replica node due to: \(error)")
        }
    }
}

extension RedisCluster {
    private func refresh(of node: RedisNode) throws {
        lock.lock()
        defer { lock.unlock() }

        logger.trace("node refresh", metadata: ["node": .string(.init(describing: node))])

        guard case let .highAvailability(_, redis) = mode else { return }

        let updated = try redis.configuration.generate(using: node)
        let newPool = makePools(using: updated)

        switch node.role {
        case .master:
            master.values
                .forEach({ $0.close(promise: nil, logger: logger) })
            replicas.values
                .flatMap(\.values)
                .forEach({ $0.close(promise: nil, logger: logger) })
            master.removeAll()
            replicas.removeAll()

            master = newPool
        case .sentinel: // TODO: update addresses
            break
        case .slave:
            if let idx = replicas.firstIndex(where: { $0.key == node.id }) {
                logger.debug("removing replica")
                let toShutdown = replicas.remove(at: idx).value
                toShutdown.values.forEach({ $0.close(promise: nil, logger: logger) })
            }

            replicas[node.id] = newPool
        }
    }

    private func makePools(using configuration: RedisMode.Configuration) -> [PoolKey: RedisConnectionPool] {
        var newPools: [PoolKey: RedisConnectionPool] = [:]

        for eventLoop in application.eventLoopGroup.makeIterator() {
            let newKey: PoolKey = eventLoop.key
            let newPool: RedisConnectionPool = makePool(using: configuration, on: eventLoop)

            newPools[newKey] = newPool
        }

        return newPools
    }

    private func makePool(using configuration: RedisMode.Configuration, on eventLoop: EventLoop) -> RedisConnectionPool {
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

        logger.debug(
            "redis pool creation",
            metadata: [
                "addresses": .string(configuration.serverAddresses.description),
                "eventLoop" : .string(eventLoop.key.debugDescription)
            ]
        )

        return RedisConnectionPool(
            configuration: .init(configuration, defaultLogger: logger, customClient: redisTLSClient),
            boundEventLoop: eventLoop
        )
    }
}

private extension EventLoop {
    typealias Key = ObjectIdentifier
    var key: Key {
        ObjectIdentifier(self)
    }
}
