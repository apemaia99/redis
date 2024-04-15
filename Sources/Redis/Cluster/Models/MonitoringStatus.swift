import Foundation

enum MonitoringStatus: String, CustomStringConvertible {
    case inactive
    case subscribing
    case active
    case unsubscribing

    case failed
    case dropped

    var description: String { rawValue }
}
