import NIOCore
import Redis
import RediStack

public struct RedisNode: RedisClusterNodeDescriptionProtocol {
    public let host: String? = nil // USELESS
    public let ip: String? = nil // USELESS
    public let useTLS: Bool = false // USELESS

    public let id: RedisClusterNodeID
    public let role: RedisRole
    public let socketAddress: SocketAddress

    public var endpoint: String {
        id.endpoint
    }

    public var port: Int {
        id.port
    }

    public init(endpoint: String, port: Int, role: RedisRole) throws {
        try self.init(from: .init(endpoint: endpoint, port: port), role: role)
    }

    public init(from id: RedisClusterNodeID, role: RedisRole) throws {
        self.id = id
        self.role = role
        socketAddress = try .makeAddressResolvingHost(id.endpoint, port: id.port)
    }
}
