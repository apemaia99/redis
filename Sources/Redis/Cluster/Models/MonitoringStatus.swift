import Foundation

enum MonitoringStatus {
    case inactive
    case subscribing
    case active
    case unsubscribing
    
    case failed
    case dropped
}
