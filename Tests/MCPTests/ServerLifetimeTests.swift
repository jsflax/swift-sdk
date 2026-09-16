import Foundation
import Logging
import Testing
@testable import MCP

/// Authored source only; all declarations UNRUN. These transports use in-memory
/// streams and explicit gates, never stdin, a service, or a room/provider.
struct ServerLifetimeTests {
    @Test func stopWaitsForEnteredHandlerDespiteCancellation() async throws {
        let entered = SDKLifetimeGate(), release = SDKLifetimeGate()
        let transport = SDKLifetimeTransport()
        let server = Server(name: "private-test", version: "1")
        await server.withMethodHandler(ListTools.self) { _ in
            await entered.open()
            await release.wait() // Intentionally ignores cancellation until released.
            return ListTools.Result(tools: [])
        }
        try await server.start(transport: transport)
        await transport.emit(request(id: "one"))
        await entered.wait()
        let stopping = Task { await server.stop() }
        await transport.disconnectEntered.wait()
        let premature = await server.isShutdownComplete
        #expect(!premature)
        await release.open()
        let joined = await stopping.value
        let writes = await transport.sendCount
        #expect(joined)
        #expect(writes == 0)
    }

    @Test func duplicateRPCIDsCannotOverwriteActualHandlerCustody() async throws {
        let entered = SDKLifetimeCounter()
        let first = SDKLifetimeGate(), second = SDKLifetimeGate()
        let transport = SDKLifetimeTransport()
        let server = Server(name: "private-test", version: "1")
        await server.withMethodHandler(ListTools.self) { _ in
            let index = await entered.record()
            if index == 1 { await first.wait() } else { await second.wait() }
            return ListTools.Result(tools: [])
        }
        try await server.start(transport: transport)
        await transport.emit(request(id: "same"))
        await entered.wait(for: 1)
        await transport.emit(request(id: "same"))
        await entered.wait(for: 2)
        let stopping = Task { await server.stop() }
        await transport.disconnectEntered.wait()
        await first.open()
        let premature = await server.isShutdownComplete
        #expect(!premature)
        await second.open()
        let joined = await stopping.value
        #expect(joined)
    }

    @Test func stopIncludesActualResponseWriteAfterHandlerReturns() async throws {
        let releaseWrite = SDKLifetimeGate()
        let transport = SDKLifetimeTransport(sendRelease: releaseWrite)
        let server = Server(name: "private-test", version: "1")
        await server.withMethodHandler(ListTools.self) { _ in ListTools.Result(tools: []) }
        try await server.start(transport: transport)
        await transport.emit(request(id: "wire"))
        await transport.sendEntered.wait(for: 1)
        let stopping = Task { await server.stop() }
        await transport.disconnectEntered.wait()
        let premature = await server.isShutdownComplete
        let activeBeforeRelease = await transport.activeSends
        #expect(!premature)
        #expect(activeBeforeRelease == 1)
        await releaseWrite.open()
        let joined = await stopping.value
        let activeAfterJoin = await transport.activeSends
        #expect(joined)
        #expect(activeAfterJoin == 0)
    }

    @Test func enteredCallbackCanCloseButCannotJoinItsOwnAncestor() async throws {
        let result = SDKLifetimeBoolean()
        let transport = SDKLifetimeTransport()
        let server = Server(name: "private-test", version: "1")
        await server.withMethodHandler(ListTools.self) { _ in
            let joined = await server.stop()
            await result.set(joined)
            return ListTools.Result(tools: [])
        }
        try await server.start(transport: transport)
        await transport.emit(request(id: "reentry"))
        await result.ready.wait()
        let callbackJoined = await result.value
        #expect(callbackJoined == false)
        let externallyJoined = await server.stop()
        #expect(externallyJoined)
    }

    @Test func closeDuringConnectJoinsTheRetainedConnectorAndRejectsStart() async throws {
        let releaseConnect = SDKLifetimeGate()
        let transport = SDKLifetimeTransport(connectRelease: releaseConnect)
        let server = Server(name: "private-test", version: "1")
        let starting = Task {
            do { try await server.start(transport: transport); return false }
            catch { return true }
        }
        await transport.connectEntered.wait()
        await server.closeAdmission()
        let stopping = Task { await server.stop() }
        let premature = await server.isShutdownComplete
        #expect(!premature)
        await releaseConnect.open()
        let rejected = await starting.value
        let joined = await stopping.value
        #expect(rejected)
        #expect(joined)
        let receives = await transport.receiveCount
        #expect(receives == 0)
    }

    @Test func nestedOwnerShutdownRetainsOriginalAncestorGuard() async throws {
        let original = Server(name: "original-private-test", version: "1")
        let nested = Server(name: "nested-private-test", version: "1")
        let originalTransport = SDKLifetimeTransport()
        let nestedTransport = SDKLifetimeTransport()
        let result = SDKLifetimeBoolean()
        await nestedTransport.setOnDisconnect {
            let joinedOriginal = await original.stop()
            await result.set(joinedOriginal)
        }
        await original.withMethodHandler(ListTools.self) { _ in
            _ = await nested.stop()
            return ListTools.Result(tools: [])
        }
        try await nested.start(transport: nestedTransport)
        try await original.start(transport: originalTransport)
        await originalTransport.emit(request(id: "nested-close"))
        await result.ready.wait()
        let nestedAttempt = await result.value
        #expect(nestedAttempt == false)
        let joined = await original.stop()
        #expect(joined)
    }

    @Test func receiveLoopCompletionIsDistinctFromHandlerDrain() async throws {
        let entered = SDKLifetimeGate(), release = SDKLifetimeGate()
        let transport = SDKLifetimeTransport()
        let server = Server(name: "private-test", version: "1")
        await server.withMethodHandler(ListTools.self) { _ in
            await entered.open()
            await release.wait()
            return ListTools.Result(tools: [])
        }
        try await server.start(transport: transport)
        await transport.emit(request(id: "eof"))
        await entered.wait()
        await transport.finishInput()
        await server.waitUntilCompleted()
        let premature = await server.isShutdownComplete
        #expect(!premature)
        let stopping = Task { await server.stop() }
        await transport.disconnectEntered.wait()
        await release.open()
        let joined = await stopping.value
        #expect(joined)
    }

    @Test func repeatedAndCancelledShutdownWaitersJoinOneActualDisconnect() async throws {
        let releaseDisconnect = SDKLifetimeGate()
        let transport = SDKLifetimeTransport(disconnectRelease: releaseDisconnect)
        let server = Server(name: "private-test", version: "1")
        try await server.start(transport: transport)
        let first = Task { await server.stop() }
        await transport.disconnectEntered.wait()
        let second = Task { await server.stop() }
        first.cancel()
        let premature = await server.isShutdownComplete
        #expect(!premature)
        await releaseDisconnect.open()
        let joinedFirst = await first.value
        let joinedSecond = await second.value
        let disconnects = await transport.disconnectCount
        #expect(joinedFirst && joinedSecond)
        #expect(disconnects == 1)
    }

    private func request(id: String) -> Data {
        Data("{\"jsonrpc\":\"2.0\",\"id\":\"\(id)\",\"method\":\"tools/list\",\"params\":{}}".utf8)
    }
}

private actor SDKLifetimeGate {
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    func wait() async {
        if opened { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    func open() {
        guard !opened else { return }
        opened = true
        let entered = waiters
        waiters.removeAll()
        for waiter in entered { waiter.resume() }
    }
}

private actor SDKLifetimeCounter {
    private var count = 0
    private var waiters: [(Int, CheckedContinuation<Void, Never>)] = []
    func record() -> Int {
        count += 1
        let ready = waiters.filter { $0.0 <= count }
        waiters.removeAll { $0.0 <= count }
        for (_, waiter) in ready { waiter.resume() }
        return count
    }
    func wait(for target: Int) async {
        if count >= target { return }
        await withCheckedContinuation { waiters.append((target, $0)) }
    }
}

private actor SDKLifetimeBoolean {
    let ready = SDKLifetimeGate()
    private(set) var value: Bool?
    func set(_ value: Bool) async {
        self.value = value
        await ready.open()
    }
}

private actor SDKLifetimeTransport: Transport {
    nonisolated let logger = Logger(label: "private.sdk-lifetime-test", factory: { _ in SwiftLogNoOpLogHandler() })
    let connectEntered = SDKLifetimeGate()
    let sendEntered = SDKLifetimeCounter()
    let disconnectEntered = SDKLifetimeGate()
    private let connectRelease: SDKLifetimeGate?
    private let sendRelease: SDKLifetimeGate?
    private let disconnectRelease: SDKLifetimeGate?
    private let stream: AsyncThrowingStream<Data, Error>
    private let continuation: AsyncThrowingStream<Data, Error>.Continuation
    private var closed = false
    private var onDisconnect: (@Sendable () async -> Void)?
    private(set) var sendCount = 0
    private(set) var activeSends = 0
    private(set) var receiveCount = 0
    private(set) var disconnectCount = 0

    init(connectRelease: SDKLifetimeGate? = nil, sendRelease: SDKLifetimeGate? = nil,
         disconnectRelease: SDKLifetimeGate? = nil) {
        self.connectRelease = connectRelease
        self.sendRelease = sendRelease
        self.disconnectRelease = disconnectRelease
        let pair = AsyncThrowingStream<Data, Error>.makeStream()
        stream = pair.stream
        continuation = pair.continuation
    }
    func connect() async throws {
        await connectEntered.open()
        if let connectRelease { await connectRelease.wait() }
    }
    func send(_ data: Data) async throws {
        guard !closed else { throw CancellationError() }
        sendCount += 1
        activeSends += 1
        defer { activeSends -= 1 }
        _ = await sendEntered.record()
        if let sendRelease { await sendRelease.wait() }
        guard !closed else { throw CancellationError() }
    }
    func receive() -> AsyncThrowingStream<Data, Error> {
        receiveCount += 1
        return stream
    }
    func disconnect() async {
        disconnectCount += 1
        closed = true
        continuation.finish()
        await disconnectEntered.open()
        if let onDisconnect { await onDisconnect() }
        if let disconnectRelease { await disconnectRelease.wait() }
    }
    func setOnDisconnect(_ callback: @escaping @Sendable () async -> Void) { onDisconnect = callback }
    func emit(_ data: Data) { continuation.yield(data) }
    func finishInput() { continuation.finish() }
}
