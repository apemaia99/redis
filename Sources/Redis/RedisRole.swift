import Foundation
import RediStack

public enum RedisRole: String {
    case master
    case slave
    case sentinel
}
