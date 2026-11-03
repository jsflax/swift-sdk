import Foundation
import Logging
import Testing
@testable import MCP

@Suite("Original typed request handlers", .timeLimit(.minutes(1)))
struct RequestHandlerTests {
    @Test func numericAndStringIDsRemainDistinctThroughActualDispatch() async throws {
        let transport = OriginalRequestTransport()
        let captured = OriginalRequestCapture()
        let server = Server(name: "request-id-fixture", version: "1")
        await server.withRequestHandler(CallTool.self) { request in
            await captured.record(request)
            return .init(content: [.text(text: request.params.name, annotations: nil, _meta: nil)])
        }
        var replies = transport.replies.makeAsyncIterator()
        let parameters = CallTool.Parameters(name: "same-call", arguments: ["value": .int(7)])
        do {
            try await server.start(transport: transport)
            let ids: [ID] = [.number(1), .string("1")]
            for id in ids {
                let request = CallTool.request(id: id, parameters)
                try await transport.emit(request)
                let next = await replies.next()
                let bytes = try #require(next)
                let response = try JSONDecoder().decode(Response<CallTool>.self, from: bytes)
                let result = try response.result.get()
                #expect(response.id == id)
                #expect(result.content == [.text(text: "same-call", annotations: nil, _meta: nil)])
            }
            let requests = await captured.requests
            #expect(requests.map(\.id) == [.number(1), .string("1")])
            #expect(requests.map(\.params) == [parameters, parameters])
        } catch { _ = await server.stop(); throw error }
        let joined = await server.stop()
        #expect(joined)
    }

    @Test func parameterAndRequestHandlersShareReplacementResultAndErrorPaths() async throws {
        let transport = OriginalRequestTransport()
        let server = Server(name: "request-parity-fixture", version: "1")
        let result = CallTool.Result(content: [.text(text: "same-result", annotations: nil, _meta: nil)])
        let parameters = CallTool.Parameters(name: "same-call", arguments: ["value": .int(7)])
        let captured = OriginalRequestCapture()
        await server.withMethodHandler(CallTool.self) { received in
            #expect(received == parameters)
            return result
        }
        var replies = transport.replies.makeAsyncIterator()
        do {
            try await server.start(transport: transport)
            try await transport.emit(CallTool.request(id: .number(9), parameters))
            let legacyNext = await replies.next()
            let legacyBytes = try #require(legacyNext)
            let legacy = try JSONDecoder().decode(Response<CallTool>.self, from: legacyBytes)
            let legacyResult = try legacy.result.get()
            #expect(legacy.id == .number(9) && legacyResult == result)

            // Same registration slot: the request-aware handler replaces the parameter handler.
            await server.withRequestHandler(CallTool.self) { request in
                await captured.record(request)
                if request.params.name == "reject" { throw MCPError.invalidParams("fixture rejection") }
                #expect(request.params == parameters)
                return result
            }
            try await transport.emit(CallTool.request(id: .string("9"), parameters))
            let typedNext = await replies.next()
            let typedBytes = try #require(typedNext)
            let typed = try JSONDecoder().decode(Response<CallTool>.self, from: typedBytes)
            let typedResult = try typed.result.get()
            #expect(typed.id == .string("9") && typedResult == legacyResult)

            try await transport.emit(CallTool.request(id: .number(10), .init(name: "reject")))
            let failedNext = await replies.next()
            let failedBytes = try #require(failedNext)
            let failed = try JSONDecoder().decode(Response<CallTool>.self, from: failedBytes)
            #expect(failed.id == .number(10))
            switch failed.result {
            case .failure(.invalidParams(let message)): #expect(message == "fixture rejection")
            default: Issue.record("The existing dispatch path must preserve the handler error")
            }
            let requests = await captured.requests
            #expect(requests.map(\.id) == [.string("9"), .number(10)])
        } catch { _ = await server.stop(); throw error }
        let joined = await server.stop()
        #expect(joined)
    }
}

private actor OriginalRequestCapture {
    private(set) var requests: [Request<CallTool>] = []
    func record(_ request: Request<CallTool>) { requests.append(request) }
}

/// Only the actual Server's input/output boundary is substituted. Both streams use bounded
/// buffers; a canceled test's reply iterator resumes, then error cleanup joins Server.stop().
private actor OriginalRequestTransport: Transport {
    nonisolated let logger = Logger(label: "mcp.request-handler.fixture", factory: { _ in SwiftLogNoOpLogHandler() })
    nonisolated let replies: AsyncStream<Data>
    private let outgoing: AsyncStream<Data>.Continuation
    private let incoming: AsyncThrowingStream<Data, any Error>
    private let input: AsyncThrowingStream<Data, any Error>.Continuation
    private var closed = false
    init() {
        let requests = AsyncThrowingStream<Data, any Error>.makeStream(bufferingPolicy: .bufferingOldest(4))
        incoming = requests.stream; input = requests.continuation
        let responses = AsyncStream<Data>.makeStream(bufferingPolicy: .bufferingOldest(4))
        replies = responses.stream; outgoing = responses.continuation
    }
    func connect() async throws { guard !closed else { throw CancellationError() } }
    func receive() -> AsyncThrowingStream<Data, any Error> { incoming }
    func send(_ bytes: Data) async throws {
        guard !closed else { throw CancellationError() }
        outgoing.yield(bytes)
    }
    func emit(_ request: Request<CallTool>) throws {
        guard !closed else { throw CancellationError() }
        input.yield(try JSONEncoder().encode(request))
    }
    func disconnect() async {
        closed = true; input.finish(); outgoing.finish()
    }
}
