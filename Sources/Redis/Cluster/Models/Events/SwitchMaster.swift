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
        guard let response = value.string else { return nil }

        let switchMaster = response.split(separator: " ")

        guard switchMaster.count == Self.expectedMessageSize else { return nil }

        let oldEndpoint = String(switchMaster[1])
        let newEndpoint = String(switchMaster[3])

        guard let oldPort = Int(switchMaster[2]),
              let newPort = Int(switchMaster[4])
        else { return nil }

        old = .init(endpoint: oldEndpoint, port: oldPort)
        new = .init(endpoint: newEndpoint, port: newPort)
    }

    func convertedToRESPValue() -> RESPValue {
        fatalError("Encoding not provided")
    }
}
