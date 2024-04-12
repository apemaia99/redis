import Foundation
import RediStack

protocol RedisClusterMonitorProviding: AnyObject {
    var delegate: RedisClusterMonitoringDelegate? { get set }

    func start(using sentinel: RedisClient)
    func stop(using sentinel: RedisClient)
}
