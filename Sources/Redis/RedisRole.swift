import Foundation
import Redis

public enum RedisRole: String, Sendable, RESPValueConvertible {
    case master
    case slave
    case sentinel

    public init?(fromRESP value: RESPValue) {
        guard let value = value.string else { return nil }
        guard let role: RedisRole = .init(rawValue: value) else { return nil }
        self = role
    }

    public func convertedToRESPValue() -> RESPValue { fatalError() }
}
