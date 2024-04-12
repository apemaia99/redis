import Foundation
import RediStack
/*
 <master name> <oldip> <oldport> <newip> <newport>
 The master new IP and address is the specified one after a configuration change.
 */
struct SwitchMaster: RESPValueConvertible {
    private static let expectedMessageSize = 5

    let old: RedisClusterNodeID
    let new: RedisClusterNodeID

    init?(fromRESP value: RESPValue) {
        guard
            let masters = value.array,
            masters.count == Self.expectedMessageSize
        else { return nil }

        guard
            let oldEndpoint = masters[1].string,
            let oldPort = masters[2].int,
            let newEndpoint = masters[3].string,
            let newPort = masters[4].int
        else { return nil }

        old = .init(endpoint: oldEndpoint, port: oldPort)
        new = .init(endpoint: newEndpoint, port: newPort)
    }

    func convertedToRESPValue() -> RESPValue {
        fatalError("Encoding not provided")
    }
}
