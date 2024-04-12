import RediStack

struct NodeInfoResponse: RESPValueConvertible {
    let info: [String: RESPValue]

    init?(fromRESP value: RESPValue) {
        guard let value = value.array, !value.isEmpty else { return nil }

        let pairs = value
            .enumerated()
            .compactMap { item -> (String, RediStack.RESPValue)? in
                guard
                    item.offset % 2 == 0,
                    let key = item.element.string
                else { return nil }

                return (key, value[item.offset + 1])
            }

        info = .init(uniqueKeysWithValues: pairs)
    }

    func convertedToRESPValue() -> RESPValue {
        let array = info.reduce(into: [RESPValue]()) { partial, item in
            partial.append(contentsOf: [.simpleString(.init(string: item.key)), item.value])
        }

        return .array(array)
    }
}
