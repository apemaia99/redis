import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL
import RediStack
import Vapor

final class RedisCluster {
    fileprivate typealias PoolKey = EventLoop.Key

    let id: RedisID
    let mode: RedisMode

    private var master: [PoolKey: RedisConnectionPool]
    private var sentinel: [PoolKey: RedisConnectionPool]
    private var replicas: [RedisClusterNodeID: [PoolKey: RedisConnectionPool]]

    private let application: Application
    private let monitoring: RedisClusterMonitorProviding
    private let lock: NIOLock

    init(id: RedisID, mode: RedisMode, application: Application, monitoring: RedisClusterMonitorProviding) {
        self.id = id
        self.mode = mode
        self.application = application
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

        application.logger.notice("REQUESTED CLIENT FOR ID: \(id), ROLE: \(role)")

        let key = eventLoop.key
        let connection: RedisConnectionPool?

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
                monitoring.start(using: sentinel)
            }
        }

        return connection
    }

    func discovery(for eventLoop: EventLoop) -> EventLoopFuture<Void> {
        application.logger.notice("START DISCOVERY for ID: \(id)")

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

        let discover = RedisTopologyDiscover(sentinel: sentinel, masterName: name, logger: application.logger)

        return discover.discovery()
            .map { nodes in
                self.application.logger.notice("END DISCOVERY")
                for node in nodes {
                    self.refresh(of: node)
                }
                self.application.logger.notice("REFRESHED TOPOLOGY")
                self.monitoring.start(using: sentinel)
            }
    }

    func bootstrap() {
        application.logger.trace("BOOTSTRAP of \(id)")
        switch mode {
        case let .standalone(configuration):
            let newPool = makePools(using: configuration)
            master = newPool
        case let .highAvailability(sentinel, _):
            let newPool = makePools(using: sentinel.configuration)
            self.sentinel = newPool
        }
    }

    func shutdown(on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        application.logger.trace("SHUTTING DOWN: \(id)")

        let master = master.values
        let sentinel = sentinel.values
        let replicas = replicas.values.flatMap(\.values)

        self.master.removeAll()
        self.sentinel.removeAll()
        self.replicas.removeAll()

        let masterShutdownFutures = master.map {
            let promise = $0.eventLoop.makePromise(of: Void.self)
            $0.close(promise: promise, logger: application.logger)
            return promise.futureResult
        }.flatten(on: eventLoop)

        let sentinelShutdownFutures = sentinel.map {
            let promise = $0.eventLoop.makePromise(of: Void.self)
            $0.close(promise: promise, logger: application.logger)
            return promise.futureResult
        }.flatten(on: eventLoop)

        let replicaShutdownFutures = replicas.map {
            let promise = $0.eventLoop.makePromise(of: Void.self)
            $0.close(promise: promise, logger: application.logger)
            return promise.futureResult
        }.flatten(on: eventLoop)

        return [masterShutdownFutures, sentinelShutdownFutures, replicaShutdownFutures]
            .flatten(on: eventLoop)
    }
}

extension RedisCluster: RedisClusterMonitoringDelegate {
    func monitoring(changed status: MonitoringStatus) {
        application.logger.notice("MONITOR: \(status) FOR: \(id)")

        guard status == .dropped, let sentinel = sentinel[application.eventLoopGroup.next().key]
        else { return }

        monitoring.start(using: sentinel)
    }

    func monitoring(shouldRefreshTopology: Bool) {
        application.logger.notice("Monitoring suggest to refresh topology: \(shouldRefreshTopology)")
        guard shouldRefreshTopology else { return }
        discovery(for: application.eventLoopGroup.next())
    }

    func switchMaster(from oldMaster: RediStack.RedisClusterNodeID, to newMaster: RediStack.RedisClusterNodeID) {
        application.logger.notice("MASTER CHANGED FROM: \(oldMaster) TO \(newMaster)")

        do {
            try refresh(of: .init(from: newMaster, role: .master))
        } catch {
            application.logger.error(
                "Cannot refresh master node due to: \(error)",
                metadata: [
                    "ID": .string(id.rawValue),
                    "NEW MASTER": .string(String(describing: newMaster)),
                ]
            )
        }
    }

    func detected(replica: RediStack.RedisClusterNodeID, relatedTo master: RediStack.RedisClusterNodeID) {
        application.logger.notice("REPLICA DETECTED: \(replica) RELATED TO MASTER: \(master)")

        do {
            try refresh(of: .init(from: replica, role: .slave))
        } catch {
            application.logger.error(
                "Cannot refresh replica node due to: \(error)",
                metadata: [
                    "ID": .string(id.rawValue),
                    "NEW REPLICA": .string(String(describing: replica)),
                    "MASTER": .string(String(describing: master)),
                ]
            )
        }
    }
}

extension RedisCluster {
    private func refresh(of node: RedisNode) {
        lock.lock()
        defer { lock.unlock() }
        application.logger.trace("refresh of \(node.role) using \(node.id)")

        guard case let .highAvailability(_, redis) = mode else { return }

        let updated = try! redis.configuration.generate(using: node)
        let newPool = makePools(using: updated)

        switch node.role {
        case .master:
            master.values
                .forEach({ $0.close(promise: nil, logger: application.logger) })
            replicas.values
                .flatMap(\.values)
                .forEach({ $0.close(promise: nil, logger: application.logger) })
            master.removeAll()
            replicas.removeAll()

            master = newPool
        case .sentinel: // TODO: update addresses
            break
        case .slave:
            if let idx = replicas.firstIndex(where: { $0.key == node.id }) {
                application.logger.notice("TURNING OFF REPLICA \(node.id)")
                let toShutdown = replicas.remove(at: idx).value
                toShutdown.values.forEach({ $0.close(promise: nil, logger: application.logger) })
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

        application.logger.notice("CREATION OF POOL CONF IP: \(configuration.serverAddresses) for id: \(id)")

        return RedisConnectionPool(
            configuration: .init(configuration, defaultLogger: application.logger, customClient: redisTLSClient),
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
