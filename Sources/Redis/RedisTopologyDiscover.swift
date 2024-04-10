import Foundation
import Redis
import RediStack
import Vapor

class RedisTopologyDiscover {
    private let sentinel: RedisClient
    private let configuration: RedisConfiguration
    private let logger: Logger

    init(sentinel: RedisClient, configuration: RedisConfiguration, logger: Logger) {
        self.sentinel = sentinel
        self.configuration = configuration
        self.logger = logger
    }
    
    deinit {
        logger.notice("DEINIT OF RedisTopologyDiscover")
    }

    func discovery(for id: RedisID) -> EventLoopFuture<RedisConfiguration> {
        let promise = sentinel.eventLoop.makePromise(of: RedisConfiguration.self)
        let master = sentinel.send(
            command: "SENTINEL",
            with: [
                "MASTER".convertedToRESPValue(),
                "mymaster".convertedToRESPValue(),
            ]
        ).flatMapThrowing { value -> RedisNode in
            guard let master = RedisClusterNodeID(fromRESP: value) else {
                throw NSError(domain: "CANNOT GET MASTER NODE", code: -1)
            }
            return try RedisNode(endpoint: master.endpoint, port: master.port, useTLS: false, role: .master)
        }

        let replicas = sentinel.send(
            command: "SENTINEL",
            with: [
                "REPLICAS".convertedToRESPValue(),
                "mymaster".convertedToRESPValue(),
            ]
        ).flatMapThrowing { value -> [RedisNode] in
            guard case let .array(replicas) = value else { return [] }

            let replicaNodes = try replicas.reduce(into: [RedisNode]()) { partial, response in
                guard let node = RedisClusterNodeID(fromRESP: response) else {
                    throw NSError(domain: "CANNOT GET REPLICAS NODES", code: -1)
                }

                let replica = try RedisNode(endpoint: node.endpoint, port: node.port, useTLS: false, role: .slave)
                partial.append(replica)
            }
            return replicaNodes
        }

        master
            .and(replicas)
            .map({ [$0] + $1 })
            .flatMapThrowing { nodes in
                self.logger.notice("NEW NODES: \(nodes)")
                try self.configuration(from: nodes)
            }

        return promise.futureResult
    }

    private func configuration(from topology: [RedisNode]) throws -> RedisConfiguration {
        guard case let .highAvailability(sentinel, redis) = configuration else {
            throw NSError(domain: "INVALID CONFIG", code: -1)
        }

        guard let master = redis.first(where: { $0.role == .master }) else {
            throw NSError(domain: "NO MASTER IN CONFIG", code: -1)
        }

        let newConfigurations = try topology.map({
            try RedisConfiguration.Configuration(
                serverAddresses: [$0.socketAddress],
                password: master.password,
                tlsConfiguration: master.tlsConfiguration,
                tlsHostname: master.tlsHostname,
                database: master.database,
                role: $0.role,
                pool: master.pool
            )
        })

        logger.notice("NEW CONFIG: \(newConfigurations)")
        return .highAvailability(sentinel: sentinel, redis: newConfigurations)
    }
}

import NIOCore
import Redis
import RediStack

public struct RedisNode: RedisClusterNodeDescriptionProtocol {
    public let host: String? = nil
    public let ip: String? = nil
    public let endpoint: String
    public let port: Int
    public let useTLS: Bool
    public let role: RedisRole

    public let socketAddress: SocketAddress

    public init(endpoint: String, port: Int = 6379, useTLS: Bool, role: RedisRole) throws {
        self.endpoint = endpoint
        self.port = port
        self.useTLS = useTLS
        self.role = role
        socketAddress = try .makeAddressResolvingHost(self.endpoint, port: self.port)
    }
}

extension RedisClusterNodeID: RESPValueConvertible {
    public init?(fromRESP value: RediStack.RESPValue) {
        guard let response = value.array, !response.isEmpty else {
            return nil
        }

        guard let endpoint = String(fromRESP: response[3]),
              let port = Int(fromRESP: response[5]) else {
            return nil
        }

        self = .init(endpoint: endpoint, port: port)
    }

    public func convertedToRESPValue() -> RediStack.RESPValue {
        let endpoint = ByteBuffer(string: endpoint)
        return .array([.simpleString(endpoint), .integer(port)])
    }
}

extension RedisClient {
    @inlinable
    public func master(_ name: RedisKey) -> EventLoopFuture<RedisClusterNodeID?> {
        return send(command: "SENTINEL GET-MASTER-ADDR-BY-NAME \(name)")
            .map { return .init(fromRESP: $0) }
    }

    @inlinable
    public func role() -> EventLoopFuture<RedisRole?> {
        return send(command: "ROLE")
            .map { return .init(fromRESP: $0) }
    }
}
