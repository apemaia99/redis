import Foundation
import NIOConcurrencyHelpers
import RediStack

final class RedisClusterMonitor: RedisClusterMonitorProviding {
    private let status: NIOLockedValueBox<MonitoringStatus>
    private let logger: Logger
    weak var delegate: RedisClusterMonitoringDelegate?

    init(logger: Logger) {
        status = .init(.inactive)
        self.logger = logger
        delegate = nil
    }

    func start(using sentinel: RedisClient) {
        let monitored = status.withLockedValue { status -> Bool in
            switch status {
            case .active:
                return true
            case .inactive:
                status = .subscribing
                // ACT AS LOCK WHILE SUBSCRIBE IS IN PROGRESS
                return false
            case .subscribing:
                return true
            }
        }

        guard !monitored else {
            logger.notice("MONITOR ALREADY ACTIVE, NOTHING TO DO")
            return
        }

        sentinel.psubscribe(to: "*") { key, message in
            self.handle(key, message)
        } onSubscribe: { subscriptionKey, _ in
            self.logger.trace("SUBSCRIBED TO \(subscriptionKey)")
            self.change(to: .active)
        } onUnsubscribe: { subscriptionKey, _ in
            self.logger.trace("UNSUBSCRIBED FROM \(subscriptionKey)")
            self.change(to: .inactive)
        }.recover { error in
            self.logger.trace("SUBSCRIBE FAILED DUE TO: \(error)")
            self.change(to: .inactive)
        }
    }

    func stop(using sentinel: RedisClient) {
        sentinel
            .punsubscribe()
            .whenSuccess {
                self.change(to: .inactive)
            }
    }
}

extension RedisClusterMonitor {
    private func change(to status: MonitoringStatus) {
        self.status.withLockedValue({ $0 = status })
        delegate?.monitoring(changed: status)
    }

    private func handle(_ publisher: RedisChannelName, _ message: RESPValue) {
        switch publisher {
        case "+switch-master":
            guard let switchMaster = SentinelEvents.SwitchMaster(fromRESP: message) else { return }
            delegate?.switchMaster(from: switchMaster.old, to: switchMaster.new)
        case "+slave":
            guard let replica = SentinelEvents.NewReplica(fromRESP: message) else { return }
            delegate?.detected(replica: replica.replica, relatedTo: replica.master)
        default:
            logger.notice("CHANNEL: \(publisher) | \(message)")
        }
    }
}
