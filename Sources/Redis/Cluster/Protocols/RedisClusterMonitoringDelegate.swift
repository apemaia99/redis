import Foundation
import RediStack

protocol RedisClusterMonitoringDelegate: AnyObject {
    func monitoring(changed status: MonitoringStatus)

    func switchMaster(from oldMaster: RedisClusterNodeID, to newMaster: RedisClusterNodeID)
    func detected(replica: RedisClusterNodeID, relatedTo master: RedisClusterNodeID)
}
