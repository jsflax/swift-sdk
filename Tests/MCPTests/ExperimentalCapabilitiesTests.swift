import Foundation
import Testing

@testable import MCP

/// Regression coverage for experimental capability decoding and public API callers.
/// The Codex-shaped frame is reconstructed from the integration selftest fields,
/// not an original wire capture.
@Suite("Experimental client capability wire decoding")
struct ExperimentalCapabilitiesTests {
    private actor Capture {
        private var value: Client.Capabilities?

        func record(_ capabilities: Client.Capabilities) {
            value = capabilities
        }

        func received() -> Client.Capabilities? { value }
    }

    /// Exercise raw Data -> AnyRequest -> typed Initialize -> Server hook.
    /// Do not construct a typed Initialize request before feeding the transport.
    private func receive(_ wire: String, id: Value) async throws -> Client.Capabilities {
        let transport = MockTransport()
        let capture = Capture()
        let server = Server(name: "CapabilityRegression", version: "1.0")
        do {
            try await server.start(transport: transport) { _, capabilities in
                await capture.record(capabilities)
            }
            await transport.queue(data: Data(wire.utf8))
            // Bound the wait so a decode failure cannot leave this test waiting
            // indefinitely for a hook that will never run.
            for _ in 0..<100 {
                if !(await transport.sentMessages).isEmpty { break }
                try await Task.sleep(for: .milliseconds(10))
            }
            let messages = await transport.sentData
            let received = await capture.received()
            await server.stop()
            await transport.disconnect()

            let response = try #require(messages.first)
            let envelope = try JSONDecoder().decode([String: Value].self, from: response)
            #expect(envelope["id"] == id)
            #expect(envelope["error"] == nil)
            #expect(envelope["result"]?.objectValue?["serverInfo"] != nil)
            return try #require(received)
        } catch {
            await server.stop()
            await transport.disconnect()
            throw error
        }
    }

    @Test("Raw Codex-shaped initialize retains auth-change empty object")
    func codexInitializeReachesHook() async throws {
        let wire = #"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{"experimental":{"codex/auth-change":{}},"elicitation":{"form":{},"url":{}}},"clientInfo":{"name":"codex-mcp-client","title":"Codex","version":"0.154.0"}}}"#
        let received = try await receive(wire, id: .int(0))
        #expect(received.experimental == ["codex/auth-change": .object([:])])
        #expect(received.elicitation?.form != nil)
        #expect(received.elicitation?.url != nil)
    }

    @Test("Raw initialize retains nested JSON and legacy string values")
    func nestedAndLegacyValuesReachHook() async throws {
        let wire = #"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{"experimental":{"nested":{"object":{"enabled":true},"items":[null,false,7,1.5,"text",{}]},"legacy":"enabled"}},"clientInfo":{"name":"capability-regression","version":"1.0"}}}"#
        let received = try await receive(wire, id: .int(1))
        let expected: [String: Value] = [
            "nested": .object([
                "object": .object(["enabled": .bool(true)]),
                "items": .array([.null, .bool(false), .int(7), .double(1.5),
                                 .string("text"), .object([:])]),
            ]),
            "legacy": .string("enabled"),
        ]
        #expect(received.experimental == expected)
        let encoded = try JSONEncoder().encode(received)
        let roundTrip = try JSONDecoder().decode(Client.Capabilities.self, from: encoded)
        #expect(roundTrip.experimental == expected)
    }

    @Test("Public capabilities API accepts legacy literals and explicitly mapped strings")
    func legacyStringAPICallers() {
        let inline = Client.Capabilities(experimental: ["legacy": "enabled"])
        #expect(inline.experimental?["legacy"]?.stringValue == "enabled")

        let strings: [String: String] = ["typed": "enabled"]
        var mapped = Client.Capabilities(experimental: strings.mapValues(Value.string))
        #expect(mapped.experimental?["typed"]?.stringValue == "enabled")

        mapped.experimental = ["literal-property": "literal"]
        #expect(mapped.experimental?["literal-property"]?.stringValue == "literal")
        mapped.experimental = strings.mapValues(Value.string)
        #expect(mapped.experimental?["typed"]?.stringValue == "enabled")
    }
}
