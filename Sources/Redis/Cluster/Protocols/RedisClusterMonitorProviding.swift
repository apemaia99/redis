import Foundation

protocol RedisClusterMonitorProviding {
    var delegate: RedisClusterMonitoringDelegate? { get set }

    func start(for id: RedisID, sentinel: RedisClient)
    func stop(for id: RedisID)
}
