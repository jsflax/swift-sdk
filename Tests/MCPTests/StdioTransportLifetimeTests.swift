import Foundation
import Testing
@testable import MCP
#if canImport(System)
import System
#else
import SystemPackage
#endif
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

#if canImport(Darwin) || canImport(Glibc) || canImport(Musl)
/// Authored UNRUN. The I/O case uses newly created test pipes only; neither case
/// borrows or closes the test runner's inherited stdin/stdout descriptors.
struct StdioTransportLifetimeTests {
    @Test func closedQueuedTransportCannotEnterDescriptorSetup() async {
        let transport = StdioTransport(input: FileDescriptor(rawValue: -1), output: FileDescriptor(rawValue: -1))
        await transport.closeAdmission()
        let joined = await transport.waitForShutdown()
        var cancelled = false
        do { try await transport.connect() }
        catch is CancellationError { cancelled = true }
        catch { Issue.record("Closed transport entered invalid descriptor setup") }
        #expect(joined)
        #expect(cancelled)
    }

    @Test func disconnectJoinsOwnedReaderAndWriteWithoutClosingPipeDescriptors() async throws {
        let input = Pipe(), output = Pipe()
        defer {
            try? input.fileHandleForReading.close()
            try? input.fileHandleForWriting.close()
            try? output.fileHandleForReading.close()
            try? output.fileHandleForWriting.close()
        }
        let inputFD = input.fileHandleForReading.fileDescriptor
        let outputFD = output.fileHandleForWriting.fileDescriptor
        let transport = StdioTransport(input: FileDescriptor(rawValue: inputFD), output: FileDescriptor(rawValue: outputFD))
        try await transport.connect()
        try await transport.send(Data("one-owned-frame".utf8))
        let joined = await transport.waitForShutdown()
        let complete = await transport.isShutdownComplete
        #expect(joined && complete)
        #expect(fcntl(inputFD, F_GETFD) >= 0)
        #expect(fcntl(outputFD, F_GETFD) >= 0)
        let expected = Data("one-owned-frame\n".utf8)
        let actual = try output.fileHandleForReading.read(upToCount: expected.count)
        #expect(actual == expected)
    }
}
#endif
