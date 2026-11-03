import Logging

import struct Foundation.Data

/// Protocol defining the transport layer for MCP communication
public protocol Transport: Actor {
    var logger: Logger { get }

    /// Establishes connection with the transport
    func connect() async throws

    /// Disconnects from the transport
    func disconnect() async

    /// Sends data
    func send(_ data: Data) async throws

    /// Receives data in an async sequence
    func receive() -> AsyncThrowingStream<Data, Swift.Error>
}

/// Optional lifecycle capability for HTTP transports and custody adapters around them.
/// The client publishes the negotiated version before sending initialized or later requests.
public protocol NegotiatedProtocolVersionTransport: Transport {
    func updateNegotiatedProtocolVersion(_ version: String) async
}
