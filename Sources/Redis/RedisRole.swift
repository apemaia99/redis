import Foundation
import RediStack

public enum RedisRole: String, CustomStringConvertible {
    case master
    case slave
    case sentinel

    public var description: String { rawValue }
}
