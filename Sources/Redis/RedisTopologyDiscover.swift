import Foundation
import Redis
import RediStack
import Vapor

class RedisTopologyDiscover {
    private let sentinel: RedisClient
    private let configuration: RedisMode.Configuration
    private let logger: Logger
    
    private var masterName: String? {
        configuration.masterName
    }

    init(sentinel: RedisClient, configuration: RedisMode.Configuration, logger: Logger) {
        self.sentinel = sentinel
        self.configuration = configuration
        self.logger = logger
    }

    deinit {
        logger.notice("DEINIT OF RedisTopologyDiscover")
    }

    func discovery(for id: RedisID) -> EventLoopFuture<[RedisNode]> {
        let master = sentinel.send(
            command: "SENTINEL",
            with: [
                "MASTER".convertedToRESPValue(),
                masterName.convertedToRESPValue(),
            ]
        ).flatMapThrowing { value -> RedisNode in
            guard let master = RedisClusterNodeID(fromRESP: value) else {
                throw NSError(domain: "CANNOT GET MASTER NODE", code: -1)
            }

            self.logger.notice("MASTER: \(master)")
            return try RedisNode(endpoint: master.endpoint, port: master.port, role: .master, previousConfiguration: self.configuration)
        }

        let replicas = sentinel.send(
            command: "SENTINEL",
            with: [
                "REPLICAS".convertedToRESPValue(),
                masterName.convertedToRESPValue(),
            ]
        ).flatMapThrowing { value -> [RedisNode] in
            guard case let .array(replicas) = value else {
                return [] // error
            }

            let replicaNodes = try replicas.reduce(into: [RedisNode]()) { partial, response in
                guard let node = RedisClusterNodeID(fromRESP: response) else {
                    throw NSError(domain: "CANNOT GET REPLICAS NODES", code: -1)
                }

                let replica = try RedisNode(endpoint: node.endpoint, port: node.port, role: .slave, previousConfiguration: self.configuration)
                partial.append(replica)
            }

            self.logger.notice("NODES: \(replicas)")
            return replicaNodes
        }

        return master
            .and(replicas)
            .map({ [$0] + $1 })
            .map { nodes in
                self.logger.notice("NEW NODES: \(nodes)")
                return nodes
            }
    }
}

import NIOCore
import Redis
import RediStack

public struct RedisNode: RedisClusterNodeDescriptionProtocol {
    public let host: String? = nil // USELESS
    public let ip: String? = nil // USELESS
    public let useTLS: Bool = false // USELESS

    public let endpoint: String
    public let port: Int
    public let role: RedisRole

    private let previousConfiguration: RedisMode.Configuration
    public let socketAddress: SocketAddress

    public init(endpoint: String, port: Int, role: RedisRole, previousConfiguration: RedisMode.Configuration) throws {
        self.endpoint = endpoint
        self.port = port
        self.role = role
        self.previousConfiguration = previousConfiguration
        socketAddress = try .makeAddressResolvingHost(self.endpoint, port: self.port)
    }

    public var configuration: RedisMode.Configuration {
        return .init(
            role: role,
            serverAddresses: [socketAddress],
            password: previousConfiguration.password,
            database: previousConfiguration.database,
            pool: previousConfiguration.pool,
            tlsConfiguration: previousConfiguration.tlsConfiguration,
            tlsHostname: previousConfiguration.tlsHostname
        )
    }
}

extension RedisNode {
    public static func == (lhs: RedisNode, rhs: RedisNode) -> Bool {
        false
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
