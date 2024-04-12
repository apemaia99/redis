import Foundation
import NIOConcurrencyHelpers

final class RedisClusterMonitor: RedisClusterMonitorProviding {
    private var status: NIOLockedValueBox<[RedisID: MonitoringStatus]>
    private var logger: Logger
    weak var delegate: RedisClusterMonitoringDelegate?

    init(logger: Logger) {
        status = .init([:])
        self.logger = logger
        delegate = nil
    }

    func start(for id: RedisID, sentinel: RedisClient) {
        sentinel.psubscribe(to: "*") { key, message in
            self.handle(key, message, for: id)
        } onSubscribe: { subscriptionKey, s in
            self.logger.trace("SUBSCRIBED TO \(subscriptionKey)")
            self.change(to: .active, for: id)
        } onUnsubscribe: { subscriptionKey, _ in
            self.logger.trace("UNSUBSCRIBED FROM \(subscriptionKey)")
            self.change(to: .inactive, for: id)
        }.recover { error in
            self.logger.trace("SUBSCRIBE FAILED DUE TO: \(error)")
            self.change(to: .inactive, for: id)
        }
    }

    func stop(for id: RedisID) {
    }
}

extension RedisClusterMonitor {
    private func change(to status: MonitoringStatus, for id: RedisID) {
        self.status.withLockedValue({ $0[id] = .active })
        delegate?.monitoring(changed: status, for: id)
    }

    private func handle(_ publisher: RedisChannelName, _ message: RESPValue, for id: RedisID) {
        switch publisher {
        case "+switch-master":
            guard let switchMaster = SwitchMaster(fromRESP: message) else { return }
            delegate?.switchMaster(from: switchMaster.old, to: switchMaster.new, for: id)
        default:
            logger.notice("CHANNEL: \(publisher) | REDIS ID: \(id) | \(message)")
        }
    }
}

// +reset-master <instance details> -- The master was reset.
// +slave <instance details> -- A new replica was detected and attached.
// +failover-state-reconf-slaves <instance details> -- Failover state changed to reconf-slaves state.
// +failover-detected <instance details> -- A failover started by another Sentinel or any other external entity was detected (An attached replica turned into a master).
// +slave-reconf-sent <instance details> -- The leader sentinel sent the REPLICAOF command to this instance in order to reconfigure it for the new replica.
// +slave-reconf-inprog <instance details> -- The replica being reconfigured showed to be a replica of the new master ip:port pair, but the synchronization process is not yet complete.
// +slave-reconf-done <instance details> -- The replica is now synchronized with the new master.
// -dup-sentinel <instance details> -- One or more sentinels for the specified master were removed as duplicated (this happens for instance when a Sentinel instance is restarted).
// +sentinel <instance details> -- A new sentinel for this master was detected and attached.
// +sdown <instance details> -- The specified instance is now in Subjectively Down state.
// -sdown <instance details> -- The specified instance is no longer in Subjectively Down state.
// +odown <instance details> -- The specified instance is now in Objectively Down state.
// -odown <instance details> -- The specified instance is no longer in Objectively Down state.
// +new-epoch <instance details> -- The current epoch was updated.
// +try-failover <instance details> -- New failover in progress, waiting to be elected by the majority.
// +elected-leader <instance details> -- Won the election for the specified epoch, can do the failover.
// +failover-state-select-slave <instance details> -- New failover state is select-slave: we are trying to find a suitable replica for promotion.
// no-good-slave <instance details> -- There is no good replica to promote. Currently we'll try after some time, but probably this will change and the state machine will abort the failover at all in this case.
// selected-slave <instance details> -- We found the specified good replica to promote.
// failover-state-send-slaveof-noone <instance details> -- We are trying to reconfigure the promoted replica as master, waiting for it to switch.
// failover-end-for-timeout <instance details> -- The failover terminated for timeout, replicas will eventually be configured to replicate with the new master anyway.
// failover-end <instance details> -- The failover terminated with success. All the replicas appears to be reconfigured to replicate with the new master.

// +tilt -- Tilt mode entered.
// -tilt -- Tilt mode exited.
