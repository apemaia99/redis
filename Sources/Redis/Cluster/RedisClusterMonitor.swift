import Foundation
import NIOConcurrencyHelpers
import RediStack

final class RedisClusterMonitor: RedisClusterMonitorProviding {
    private let status: NIOLockedValueBox<Status>
    private let logger: Logger
    weak var delegate: RedisClusterMonitoringDelegate?

    private struct Status {
        private(set) var previousState: MonitoringStatus
        private(set) var currentState: MonitoringStatus

        mutating func set(state: MonitoringStatus) {
            previousState = currentState
            currentState = state
        }
    }

    init(logger: Logger) {
        status = .init(.init(previousState: .inactive, currentState: .inactive))
        self.logger = logger
    }

    func start(using sentinel: RedisClient) {
        let shouldSubscribe = status.withLockedValue { fsm -> Bool in
            switch fsm.currentState {
            case .inactive, .failed, .dropped:
                fsm.set(state: .subscribing)
                return true
            case .active, .subscribing, .unsubscribing:
                return false
            }
        }

        guard shouldSubscribe else {
            return logger.debug("Already monitoring")
        }

        notify(.subscribing)

        sentinel.psubscribe(
            to: "*",
            messageReceiver: handle,
            onSubscribe: subscribed,
            onUnsubscribe: unsubscribed
        ).recover { error in
            self.logger.trace("SUBSCRIBE FAILED DUE TO: \(error)")
            self.change(to: .failed)
        }
    }

    func stop(using sentinel: RedisClient) {
        change(to: .unsubscribing)
        sentinel.punsubscribe()
            .recover { error in
                self.logger.trace("UNSUBSCRIBE FAILED DUE TO: \(error)")
                self.change(to: .inactive)
            }
    }
}

extension RedisClusterMonitor {
    private func subscribed(_ subscriptionKey: String, _ currentSubscriptionCount: Int) {
        logger.trace("SUBSCRIBED TO \(subscriptionKey) | count \(currentSubscriptionCount)")
        switch status.withLockedValue({ $0.previousState }) {
        case .failed:
            change(to: .active)
            delegate?.monitoring(shouldRefreshTopology: true)
        default:
            change(to: .active)
            delegate?.monitoring(shouldRefreshTopology: false)
        }
    }

    private func unsubscribed(_ subscriptionKey: String, _ currentSubscriptionCount: Int) {
        logger.trace("UNSUBSCRIBED FROM \(subscriptionKey) | count \(currentSubscriptionCount)")
        switch status.withLockedValue({ $0.currentState }) {
        case .unsubscribing:
            change(to: .inactive)
        default:
            change(to: .dropped)
        }
    }
}

extension RedisClusterMonitor {
    private func change(to status: MonitoringStatus) {
        self.status.withLockedValue({ $0.set(state: status) })
        notify(status)
    }

    private func notify(_ status: MonitoringStatus) {
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
