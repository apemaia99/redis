import RediStack
import Vapor

extension RedisClusterNodeID {
    init?(from nodeInfo: NodeInfoResponse) {
        let info = nodeInfo.info

        guard
            let endpoint = info["ip"]?.string,
            let port = info["port"]?.int
        else { return nil }

        self = .init(endpoint: endpoint, port: port)
    }
}
