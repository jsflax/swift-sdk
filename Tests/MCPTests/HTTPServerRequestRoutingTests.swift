import Foundation
import Logging
import Testing

@testable import MCP

/// Real Server dispatch and HTTP transport routing; no sockets or readiness sleeps.
struct HTTPServerRequestRoutingTests {
    @Test(arguments: [ID.string("origin"), ID.number(42)])
    func handlerElicitationUsesOnlyOriginatingPOST(_ originID: ID) async throws {
        try await withRoutingHarness { harness in
            let unrelatedEntered = RoutingGate(), releaseUnrelated = RoutingGate()
            await harness.server.withMethodHandler(CallTool.self) { params in
                if params.name == "unrelated" {
                    unrelatedEntered.open()
                    try await releaseUnrelated.wait()
                    return .init(content: [.text("unrelated complete")])
                }
                let result = try await harness.server.requestElicitation(
                    message: "Origin question", requestedSchema: .init())
                #expect(result.action == .accept)
                return .init(content: [.text("origin complete")])
            }
            let session = try await harness.initialize()
            var unrelated = try RoutingSSE(await harness.call(
                "unrelated", id: .string("unrelated"), session: session))
            try await unrelatedEntered.wait()

            // No GET exists. A different POST is already live when this handler sends.
            var origin = try RoutingSSE(await harness.call("origin", id: originID, session: session))
            let elicitation = try JSONDecoder().decode(
                Request<CreateElicitation>.self, from: try await origin.nextMessage())
            #expect(elicitation.method == CreateElicitation.name)
            let accepted = try await harness.accept(elicitation.id, session: session)
            #expect(accepted.statusCode == 202)
            let final = try JSONDecoder().decode(
                Response<CallTool>.self, from: try await origin.nextMessage())
            #expect(final.id == originID)
            #expect(try final.result.get().content == [.text("origin complete")])
            #expect(try await origin.nextEvent() == nil)

            releaseUnrelated.open()
            let unrelatedMessages = try await unrelated.drainMessages()
            #expect(unrelatedMessages.count == 1)
            let unrelatedFinal = try JSONDecoder().decode(
                Response<CallTool>.self, from: try #require(unrelatedMessages.first))
            #expect(unrelatedFinal.id == .string("unrelated"))
            #expect(try unrelatedFinal.result.get().content == [.text("unrelated complete")])
        }
    }

    @Test
    func disconnectedOriginRetainsElicitationForItsReplay() async throws {
        try await withRoutingHarness { harness in
            let entered = RoutingGate(), release = RoutingGate()
            await harness.server.withMethodHandler(CallTool.self) { _ in
                entered.open()
                try await release.wait()
                let result = try await harness.server.requestElicitation(
                    message: "Replay question", requestedSchema: .init())
                #expect(result.action == .accept)
                return .init(content: [.text("replay complete")])
            }
            let session = try await harness.initialize()
            var standalone = try RoutingSSE(await harness.get(session: session))
            _ = try #require(try await standalone.nextEvent()) // GET priming event
            var origin = try RoutingSSE(await harness.call(
                "origin", id: .string("replay-origin"), session: session))
            let priming = try #require(try await origin.nextEvent())
            let lastEventID = try #require(priming.id)
            #expect(priming.data == nil)
            try await entered.wait()

            // The existing SEP-1699 control removes the POST continuation.
            await harness.transport.closeSSEStream(forRequestID: "replay-origin")
            #expect(try await origin.nextEvent() == nil)
            release.open()
            // Positive evidence: the real transport.send returned while no origin
            // continuation existed. The probe never routes or fabricates data.
            var writes = harness.probe.requestWrites.makeAsyncIterator()
            let written = try #require(await writes.next())
            let writtenRequest = try JSONDecoder().decode(Request<CreateElicitation>.self, from: written)

            var replay = try RoutingSSE(await harness.get(session: session, lastEventID: lastEventID))
            let replayed = try await replay.nextMessage()
            #expect(replayed == written)
            let accepted = try await harness.accept(writtenRequest.id, session: session)
            #expect(accepted.statusCode == 202)
            let final = try JSONDecoder().decode(
                Response<CallTool>.self, from: try await replay.nextMessage())
            #expect(final.id == .string("replay-origin"))
            #expect(try final.result.get().content == [.text("replay complete")])
            #expect(try await replay.nextEvent() == nil)

            // Finish the independent GET, then inspect its complete contents.
            #expect(await harness.server.stop())
            #expect(try await standalone.drainMessages().isEmpty)
        }
    }

    @Test
    func unsolicitedElicitationStillUsesStandaloneGET() async throws {
        try await withRoutingHarness { harness in
            let entered = RoutingGate(), release = RoutingGate()
            await harness.server.withMethodHandler(CallTool.self) { _ in
                entered.open()
                try await release.wait()
                return .init(content: [.text("unrelated complete")])
            }
            let session = try await harness.initialize()
            var unrelated = try RoutingSSE(await harness.call(
                "unrelated", id: .number(99), session: session))
            try await entered.wait()
            var standalone = try RoutingSSE(await harness.get(session: session))
            // This call originates in the test lifetime, outside every handler.
            async let result = harness.server.requestElicitation(
                message: "Unsolicited question", requestedSchema: .init())
            let elicitation = try JSONDecoder().decode(
                Request<CreateElicitation>.self, from: try await standalone.nextMessage())
            let accepted = try await harness.accept(elicitation.id, session: session)
            #expect(accepted.statusCode == 202)
            let unsolicitedResult = try await result
            #expect(unsolicitedResult.action == .accept)

            release.open()
            let messages = try await unrelated.drainMessages()
            #expect(messages.count == 1)
            let final = try JSONDecoder().decode(
                Response<CallTool>.self, from: try #require(messages.first))
            #expect(final.id == .number(99))
            #expect(try final.result.get().content == [.text("unrelated complete")])
            #expect(await harness.server.stop())
            #expect(try await standalone.drainMessages().isEmpty)
        }
    }

    @Test(arguments: [ID.string("shared"), ID.string("different")])
    func foreignHandlerRequestDoesNotClaimOtherServersPOST(_ parkedID: ID) async throws {
        try await withRoutingHarnesses(count: 2) { harnesses in
            let caller = harnesses[0], target = harnesses[1]
            let parkedEntered = RoutingGate(), releaseParked = RoutingGate()
            await target.server.withMethodHandler(CallTool.self) { _ in
                parkedEntered.open()
                try await releaseParked.wait()
                return .init(content: [.text("target parked complete")])
            }
            await caller.server.withMethodHandler(CallTool.self) { _ in
                // The caller's inherited handler ID belongs to a different
                // server, even when it matches the target's parked POST ID.
                let result = try await target.server.requestElicitation(
                    message: "Cross-server question", requestedSchema: .init())
                #expect(result.action == .accept)
                return .init(content: [.text("caller complete")])
            }
            let callerSession = try await caller.initialize()
            let targetSession = try await target.initialize()
            var parked = try RoutingSSE(await target.call(
                "parked", id: parkedID, session: targetSession))
            try await parkedEntered.wait()
            var targetGET = try RoutingSSE(await target.get(session: targetSession))
            var origin = try RoutingSSE(await caller.call(
                "cross-server", id: .string("shared"), session: callerSession))

            let elicitation = try JSONDecoder().decode(
                Request<CreateElicitation>.self, from: try await targetGET.nextMessage())
            #expect(elicitation.method == CreateElicitation.name)
            let accepted = try await target.accept(elicitation.id, session: targetSession)
            #expect(accepted.statusCode == 202)

            // Drain the caller POST to EOF: it receives only its own result.
            let callerMessages = try await origin.drainMessages()
            #expect(callerMessages.count == 1)
            let callerFinal = try JSONDecoder().decode(
                Response<CallTool>.self, from: try #require(callerMessages.first))
            #expect(callerFinal.id == .string("shared"))
            #expect(try callerFinal.result.get().content == [.text("caller complete")])

            releaseParked.open()
            let parkedMessages = try await parked.drainMessages()
            #expect(parkedMessages.count == 1)
            let parkedFinal = try JSONDecoder().decode(
                Response<CallTool>.self, from: try #require(parkedMessages.first))
            #expect(parkedFinal.id == parkedID)
            #expect(try parkedFinal.result.get().content == [.text("target parked complete")])
            #expect(await target.server.stop())
            #expect(try await targetGET.drainMessages().isEmpty)
        }
    }
}

private enum RoutingFailure: Error { case timeout, expectedStream, missingMessage }

/// One waiter per gate. AsyncStream cancellation releases failed-test waiters.
private struct RoutingGate: Sendable {
    private let stream: AsyncStream<Void>
    private let continuation: AsyncStream<Void>.Continuation
    init() { (stream, continuation) = AsyncStream<Void>.makeStream() }
    func open() { continuation.yield(()); continuation.finish() }
    func wait() async throws {
        var iterator = stream.makeAsyncIterator()
        guard await iterator.next() != nil else { throw CancellationError() }
        try Task.checkCancellation()
    }
}

/// Observes completed writes without changing task locals, bytes, or routing.
private actor RoutingProbe: Transport {
    nonisolated let logger: Logger
    nonisolated let requestWrites: AsyncStream<Data>
    private let written: AsyncStream<Data>.Continuation
    private let transport: StatefulHTTPServerTransport
    private let incoming: AsyncThrowingStream<Data, Error>

    init(transport: StatefulHTTPServerTransport, incoming: AsyncThrowingStream<Data, Error>) {
        self.transport = transport
        self.incoming = incoming
        logger = transport.logger
        (requestWrites, written) = AsyncStream<Data>.makeStream()
    }
    func connect() async throws { try await transport.connect() }
    func disconnect() async { await transport.disconnect(); written.finish() }
    func receive() -> AsyncThrowingStream<Data, Error> { incoming }
    func send(_ data: Data) async throws {
        try await transport.send(data)
        if let kind = JSONRPCMessageKind(data: data), case .request = kind { written.yield(data) }
    }
}

private struct RoutingHarness: Sendable {
    let transport: StatefulHTTPServerTransport
    let probe: RoutingProbe
    let server: Server

    func initialize() async throws -> String {
        try await server.start(transport: probe)
        let message: [String: Any] = [
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": [
                "protocolVersion": "2025-11-25",
                "clientInfo": ["name": "routing-test", "version": "1"],
                "capabilities": ["elicitation": ["form": [:] as [String: Any]]],
            ] as [String: Any],
        ]
        let body = try JSONSerialization.data(withJSONObject: message)
        let response = await transport.handleRequest(post(body))
        let session = try #require(response.headers[HTTPHeaderName.sessionID])
        var stream = try RoutingSSE(response)
        let initialized = try JSONDecoder().decode(
            Response<Initialize>.self, from: try await stream.nextMessage())
        #expect(initialized.id == .number(1))
        _ = try initialized.result.get()
        #expect(try await stream.nextEvent() == nil)
        let notification = Data(#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.utf8)
        let accepted = await transport.handleRequest(post(notification, session: session))
        #expect(accepted.statusCode == 202)
        return session
    }

    func call(_ name: String, id: ID, session: String) async throws -> HTTPResponse {
        let body = try JSONEncoder().encode(CallTool.request(id: id, .init(name: name)))
        return await transport.handleRequest(post(body, session: session))
    }

    func accept(_ id: ID, session: String) async throws -> HTTPResponse {
        let body = try JSONEncoder().encode(CreateElicitation.response(id: id, result: .init(action: .accept)))
        return await transport.handleRequest(post(body, session: session))
    }

    func get(session: String, lastEventID: String? = nil) async -> HTTPResponse {
        var headers = ["Accept": "text/event-stream", "Mcp-Session-Id": session]
        if let lastEventID { headers["Last-Event-ID"] = lastEventID }
        return await transport.handleRequest(HTTPRequest(method: "GET", headers: headers))
    }

    private func post(_ data: Data, session: String? = nil) -> HTTPRequest {
        var headers = ["Content-Type": "application/json", "Accept": "application/json, text/event-stream"]
        if let session { headers["Mcp-Session-Id"] = session }
        return HTTPRequest(method: "POST", headers: headers, body: data)
    }
}

private struct RoutingEvent: Equatable {
    let id: String?
    let data: Data?
}

/// The transport yields one complete formatted SSE event per chunk.
private struct RoutingSSE {
    private var iterator: AsyncThrowingStream<Data, Error>.Iterator
    init(_ response: HTTPResponse) throws {
        guard case .stream(let stream, _) = response else { throw RoutingFailure.expectedStream }
        iterator = stream.makeAsyncIterator()
    }
    mutating func nextEvent() async throws -> RoutingEvent? {
        guard let chunk = try await iterator.next() else { return nil }
        let lines = String(decoding: chunk, as: UTF8.self).split(separator: "\n")
        let id = lines.first(where: { $0.hasPrefix("id:") }).map {
            String($0.dropFirst(3)).trimmingCharacters(in: .whitespaces)
        }
        let payload = lines.filter { $0.hasPrefix("data:") }.map {
            String($0.dropFirst(5)).trimmingCharacters(in: .whitespaces)
        }.joined(separator: "\n")
        return RoutingEvent(id: id, data: payload.isEmpty ? nil : Data(payload.utf8))
    }
    mutating func nextMessage() async throws -> Data {
        while let event = try await nextEvent() {
            if let data = event.data { return data }
        }
        throw RoutingFailure.missingMessage
    }
    mutating func drainMessages() async throws -> [Data] {
        var messages: [Data] = []
        while let event = try await nextEvent() {
            if let data = event.data { messages.append(data) }
        }
        return messages
    }
}

/// The timer is only a failure bound. Readiness always uses real stream events
/// or explicit gates. Stop closes pending request continuations before group join.
private func withRoutingHarness(
    _ body: @escaping @Sendable (RoutingHarness) async throws -> Void
) async throws {
    try await withRoutingHarnesses(count: 1) { harnesses in
        try await body(harnesses[0])
    }
}

private func withRoutingHarnesses(
    count: Int,
    _ body: @escaping @Sendable ([RoutingHarness]) async throws -> Void
) async throws {
    var created: [RoutingHarness] = []
    for _ in 0..<count {
        let transport = StatefulHTTPServerTransport(
            validationPipeline: StandardValidationPipeline(validators: []))
        let probe = RoutingProbe(transport: transport, incoming: await transport.receive())
        created.append(RoutingHarness(
            transport: transport, probe: probe, server: Server(name: "routing-test", version: "1")))
    }
    let harnesses = created
    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask { try await body(harnesses) }
        group.addTask {
            try await Task.sleep(for: .seconds(10))
            throw RoutingFailure.timeout
        }
        do {
            _ = try await group.next()
            group.cancelAll()
            await stopRoutingHarnesses(harnesses)
        } catch {
            group.cancelAll()
            await stopRoutingHarnesses(harnesses)
            throw error
        }
    }
}

private func stopRoutingHarnesses(_ harnesses: [RoutingHarness]) async {
    // Close every owner before joining any one: an A handler may still await
    // B's pending elicitation, so joining A before fencing B could deadlock.
    for harness in harnesses { await harness.server.closeAdmission() }
    for harness in harnesses { #expect(await harness.server.stop()) }
}
