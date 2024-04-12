import Foundation
import Logging
import NIOCore
import NIOPosix
import NIOSSL
import RediStack

// How redis works
public enum RedisMode {
    public typealias PoolOptions = Configuration.PoolOptions
    public typealias ValidationError = RedisConnection.Configuration.ValidationError

    case standalone(Redis.RedisMode.Configuration)
    case highAvailability(sentinel: Redis.RedisMode.SentinelConfiguration, redis: Redis.RedisMode.RedisConfiguration)
}

// Configuration: TODO protocol or classes
extension RedisMode {
    /// Configuration for connecting to a Redis instance
    public struct Configuration {
        public internal(set) var role: RedisRole
        public internal(set) var serverAddresses: [SocketAddress]
        public let password: String?
        public let database: Int?
        public let pool: PoolOptions
        public let tlsConfiguration: TLSConfiguration?
        public let tlsHostname: String?
        public let masterName: String?

        public init(
            role: RedisRole,
            serverAddresses: [SocketAddress],
            password: String? = nil,
            database: Int? = nil,
            pool: PoolOptions,
            tlsConfiguration: TLSConfiguration? = nil,
            tlsHostname: String? = nil,
            masterName: String? = nil
        ) {
            self.role = role
            self.serverAddresses = serverAddresses
            self.password = password
            self.database = database
            self.pool = pool
            self.tlsConfiguration = tlsConfiguration
            self.tlsHostname = tlsHostname
            self.masterName = masterName
        }
        
        mutating func update(using node: RedisClusterNodeID, as role: RedisRole) throws {
            self.role = role
            serverAddresses = try [.makeAddressResolvingHost(node.endpoint, port: node.port)]
        }
    }

    /// Configuration for connecting to a Redis sentinel
    public struct SentinelConfiguration {
        public let configuration: RedisMode.Configuration

        public init(
            serverAddresses: [SocketAddress],
            password: String? = nil,
            pool: PoolOptions = .init(),
            tlsConfiguration: TLSConfiguration? = nil,
            tlsHostname: String? = nil,
            masterName: String = "mymaster"
        ) {
            configuration = .init(
                role: .sentinel,
                serverAddresses: serverAddresses,
                password: password,
                database: nil,
                pool: pool,
                tlsConfiguration: tlsConfiguration,
                tlsHostname: tlsHostname,
                masterName: masterName
            )
        }
    }

    /// Configuration for connecting to a Redis standard after sentinel discovery
    public struct RedisConfiguration {
        public let configuration: RedisMode.Configuration

        public init(
            password: String? = nil,
            database: Int? = nil,
            pool: PoolOptions = .init(),
            tlsConfiguration: TLSConfiguration? = nil,
            tlsHostname: String? = nil
        ) {
            configuration = .init(
                role: .master,
                serverAddresses: [],
                password: password,
                database: database,
                pool: pool,
                tlsConfiguration: tlsConfiguration,
                tlsHostname: tlsHostname
            )
        }
    }
}

// Factories
extension RedisMode {
    public init(url string: String, tlsConfiguration: TLSConfiguration? = nil, pool: PoolOptions = .init()) throws {
        guard let url = URL(string: string) else { throw ValidationError.invalidURLString }
        try self.init(url: url, tlsConfiguration: tlsConfiguration, pool: pool)
    }

    public init(url: URL, tlsConfiguration: TLSConfiguration? = nil, pool: PoolOptions = .init()) throws {
        guard
            let scheme = url.scheme,
            !scheme.isEmpty
        else { throw ValidationError.missingURLScheme }
        guard scheme == "redis" || scheme == "rediss" else { throw ValidationError.invalidURLScheme }
        guard let host = url.host, !host.isEmpty else { throw ValidationError.missingURLHost }

        let defaultTLSConfig: TLSConfiguration?
        if scheme == "rediss" {
            // If we're given a 'rediss' URL, make sure we have at least a default TLS config.
            defaultTLSConfig = tlsConfiguration ?? .makeClientConfiguration()
        } else {
            defaultTLSConfig = tlsConfiguration
        }

        try self.init(
            hostname: host,
            port: url.port ?? RedisConnection.Configuration.defaultPort,
            password: url.password,
            tlsConfiguration: defaultTLSConfig,
            database: Int(url.lastPathComponent),
            pool: pool
        )
    }

    public init(
        hostname: String,
        port: Int = RedisConnection.Configuration.defaultPort,
        password: String? = nil,
        tlsConfiguration: TLSConfiguration? = nil,
        database: Int? = nil,
        pool: PoolOptions = .init()
    ) throws {
        if database != nil && database! < 0 { throw ValidationError.outOfBoundsDatabaseID }

        try self.init(
            serverAddresses: [.makeAddressResolvingHost(hostname, port: port)],
            password: password,
            tlsConfiguration: tlsConfiguration,
            tlsHostname: hostname,
            database: database,
            pool: pool
        )
    }

    public init(
        serverAddresses: [SocketAddress],
        password: String? = nil,
        tlsConfiguration: TLSConfiguration? = nil,
        tlsHostname: String? = nil,
        database: Int? = nil,
        role: RedisRole = .master,
        pool: PoolOptions = .init()
    ) throws {
        self = .standalone(
            .init(
                role: role,
                serverAddresses: serverAddresses,
                password: password,
                database: database,
                pool: pool,
                tlsConfiguration: tlsConfiguration,
                tlsHostname: tlsHostname
            )
        )
    }
}

extension RedisMode.Configuration {
    /// Configuration pool options
    public struct PoolOptions {
        public var maximumConnectionCount: RedisConnectionPoolSize
        public var minimumConnectionCount: Int
        public var connectionBackoffFactor: Float32
        public var initialConnectionBackoffDelay: TimeAmount
        public var connectionRetryTimeout: TimeAmount?
        public var onUnexpectedConnectionClose: ((RedisConnection) -> Void)?

        public init(
            maximumConnectionCount: RedisConnectionPoolSize = .maximumActiveConnections(2),
            minimumConnectionCount: Int = 0,
            connectionBackoffFactor: Float32 = 2,
            initialConnectionBackoffDelay: TimeAmount = .milliseconds(100),
            connectionRetryTimeout: TimeAmount? = nil,
            onUnexpectedConnectionClose: ((RedisConnection) -> Void)? = nil
        ) {
            self.maximumConnectionCount = maximumConnectionCount
            self.minimumConnectionCount = minimumConnectionCount
            self.connectionBackoffFactor = connectionBackoffFactor
            self.initialConnectionBackoffDelay = initialConnectionBackoffDelay
            self.connectionRetryTimeout = connectionRetryTimeout
            self.onUnexpectedConnectionClose = onUnexpectedConnectionClose
        }
    }
}

extension RedisConnectionPool.Configuration {
    internal init(_ config: RedisMode.Configuration, defaultLogger: Logger, customClient: ClientBootstrap?) {
        self.init(
            initialServerConnectionAddresses: config.serverAddresses,
            maximumConnectionCount: config.pool.maximumConnectionCount,
            connectionFactoryConfiguration: .init(
                connectionInitialDatabase: config.database,
                connectionPassword: config.password,
                connectionDefaultLogger: defaultLogger,
                tcpClient: customClient
            ),
            minimumConnectionCount: config.pool.minimumConnectionCount,
            connectionBackoffFactor: config.pool.connectionBackoffFactor,
            initialConnectionBackoffDelay: config.pool.initialConnectionBackoffDelay,
            connectionRetryTimeout: config.pool.connectionRetryTimeout,
            onUnexpectedConnectionClose: config.pool.onUnexpectedConnectionClose,
            poolDefaultLogger: defaultLogger
        )
    }
}
