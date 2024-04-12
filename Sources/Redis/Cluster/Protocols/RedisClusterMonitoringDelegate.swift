import Foundation

protocol RedisClusterMonitoringDelegate: AnyObject {
    func monitoring(changed status: MonitoringStatus, for id: RedisID)

    func switchMaster(from oldMaster: RedisClusterNodeID, to newMaster: RedisClusterNodeID, for id: RedisID)
}
