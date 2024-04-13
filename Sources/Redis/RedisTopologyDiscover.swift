import Foundation
import Redis
import RediStack
import Vapor

class RedisTopologyDiscover {
    private let sentinel: RedisClient
    private let masterName: String
    private let logger: Logger

    init(sentinel: RedisClient, masterName: String, logger: Logger) {
        self.sentinel = sentinel
        self.masterName = masterName
        self.logger = logger
    }

    func discovery() -> EventLoopFuture<[RedisNode]> {
        let master = master()
        let replicas = replicas()

        return master
            .and(replicas)
            .map({ [$0] + $1 })
            .map({ nodes in
                self.logger.notice("NEW NODES: \(nodes)")
                return nodes
            })
    }

    func master() -> EventLoopFuture<RedisNode> {
        sentinel.send(
            command: "SENTINEL",
            with: [
                "MASTER".convertedToRESPValue(),
                masterName.convertedToRESPValue(),
            ]
        ).flatMapThrowing { value -> RedisNode in
            guard
                let response = NodeInfoResponse(fromRESP: value),
                let node = RedisClusterNodeID(from: response)
            else {
                throw NSError(domain: "CANNOT GET MASTER NODE", code: -1)
            }

            self.logger.notice("MASTER: \(node)")
            return try .init(from: node, role: .master)
        }
    }

    func replicas() -> EventLoopFuture<[RedisNode]> {
        sentinel.send(
            command: "SENTINEL",
            with: [
                "REPLICAS".convertedToRESPValue(),
                masterName.convertedToRESPValue(),
            ]
        ).flatMapThrowing { value -> [RedisNode] in
            guard case let .array(replicas) = value else { return [] }

            let replicaNodes = try replicas.reduce(into: [RedisNode]()) { partial, response in
                guard
                    let infoResponse = NodeInfoResponse(fromRESP: response),
                    let node = RedisClusterNodeID(from: infoResponse)
                else {
                    throw NSError(domain: "CANNOT GET MASTER NODE", code: -1)
                }

                try partial.append(.init(from: node, role: .slave))
            }

            self.logger.notice("NODES: \(replicas)")
            return replicaNodes
        }
    }
}

