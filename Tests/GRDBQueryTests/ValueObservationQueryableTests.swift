import Combine
import XCTest
import GRDB
import GRDBQuery

class ValueObservationQueryableTests: XCTestCase {
    @MainActor private func publisher<Request: Queryable>(
        for request: Request,
        in context: Request.Context,
        subscription: QuerySubscription
    ) throws -> Request.ValuePublisher {
        try request.publisher(in: context, subscription: subscription)
    }

    @MainActor private func valuesAreEquivalent<Request: Queryable>(
        _ request: Request.Type,
        _ lhs: Request.Value,
        _ rhs: Request.Value
    ) -> Bool {
        Request.valuesAreEquivalent(lhs, rhs)
    }

    @MainActor func test_initial_value_is_fetched_immediately() throws {
        struct Request: ValueObservationQueryable {
            static var defaultValue: Bool { false }
            
            func fetch(_ db: Database) throws -> Bool {
                true
            }
        }
        
        let request = Request()
        let dbQueue = try DatabaseQueue()
        let context = DatabaseContext.readOnly { dbQueue }
        let publisher = request.publisher(in: context)
        
        let valueMutex = Mutex(false)
        _ = publisher.sink { completion in
            if case .failure = completion { XCTFail() }
        } receiveValue: { value in
            valueMutex.withLock { $0 = value }
        }
        XCTAssertTrue(valueMutex.withLock { $0 })
    }
    
    @MainActor func test_initial_value_is_not_fetched_immediately_and_received_on_main_actor_with_async_option() throws {
        struct Request: ValueObservationQueryable {
            static let queryableOptions = QueryableOptions.async
            static var defaultValue: Bool { false }
            
            func fetch(_ db: Database) throws -> Bool {
                true
            }
        }
        
        let request = Request()
        let dbQueue = try DatabaseQueue()
        let context = DatabaseContext.readOnly { dbQueue }
        let publisher = request.publisher(in: context)
        
        let valueMutex = Mutex(false)
        let expectation = expectation(description: "value")
        let cancellable = publisher.sink { completion in
            if case .failure = completion { XCTFail() }
        } receiveValue: { value in
            MainActor.assumeIsolated {
                valueMutex.withLock { $0 = value }
                expectation.fulfill()
            }
        }
        XCTAssertFalse(valueMutex.withLock { $0 })
        withExtendedLifetime(cancellable) {
            wait(for: [expectation])
        }
        XCTAssertTrue(valueMutex.withLock { $0 })
    }

    @MainActor func test_async_on_resume_keeps_initial_fetch_immediate() throws {
        struct Request: ValueObservationQueryable {
            static let queryableOptions = QueryableOptions.asyncOnResume
            static var defaultValue: Bool { false }

            func fetch(_ db: Database) throws -> Bool {
                true
            }
        }

        let request = Request()
        let dbQueue = try DatabaseQueue()
        let context = DatabaseContext.readOnly { dbQueue }
        let publisher = try publisher(for: request, in: context, subscription: .initial)

        let valueMutex = Mutex(false)
        _ = publisher.sink { completion in
            if case .failure = completion { XCTFail() }
        } receiveValue: { value in
            valueMutex.withLock { $0 = value }
        }
        XCTAssertTrue(valueMutex.withLock { $0 })
    }

    @MainActor func test_async_on_resume_fetches_resumed_value_asynchronously() throws {
        struct Request: ValueObservationQueryable {
            static let queryableOptions = QueryableOptions.asyncOnResume
            static var defaultValue: Bool { false }

            func fetch(_ db: Database) throws -> Bool {
                true
            }
        }

        let request = Request()
        let dbQueue = try DatabaseQueue()
        let context = DatabaseContext.readOnly { dbQueue }
        let publisher = try publisher(for: request, in: context, subscription: .resuming)

        let valueMutex = Mutex(false)
        let expectation = expectation(description: "value")
        let cancellable = publisher.sink { completion in
            if case .failure = completion { XCTFail() }
        } receiveValue: { value in
            MainActor.assumeIsolated {
                valueMutex.withLock { $0 = value }
                expectation.fulfill()
            }
        }
        XCTAssertFalse(valueMutex.withLock { $0 })
        withExtendedLifetime(cancellable) {
            wait(for: [expectation])
        }
        XCTAssertTrue(valueMutex.withLock { $0 })
    }

    @MainActor func test_remove_duplicates_compares_values_across_subscriptions() {
        struct Request: ValueObservationQueryable {
            static let queryableOptions = QueryableOptions.removeDuplicates
            static var defaultValue: Int { 0 }

            func fetch(_ db: Database) throws -> Int {
                0
            }
        }

        XCTAssertTrue(valuesAreEquivalent(Request.self, 1, 1))
        XCTAssertFalse(valuesAreEquivalent(Request.self, 1, 2))
    }

    @MainActor func test_values_are_not_compared_without_remove_duplicates_option() {
        struct Request: ValueObservationQueryable {
            static var defaultValue: Int { 0 }

            func fetch(_ db: Database) throws -> Int {
                0
            }
        }

        XCTAssertFalse(valuesAreEquivalent(Request.self, 1, 1))
    }

    @MainActor func test_custom_context() throws {
        struct DatabaseManager: TopLevelDatabaseReader {
            var reader: any DatabaseReader
        }
        struct Request: ValueObservationQueryable {
            typealias Context = DatabaseManager
            static var defaultValue: Bool { false }
            
            func fetch(_ db: Database) throws -> Bool {
                true
            }
        }
        
        let request = Request()
        let dbQueue = try DatabaseQueue()
        let manager = DatabaseManager(reader: dbQueue)
        let publisher = request.publisher(in: manager)
        
        let valueMutex = Mutex(false)
        _ = publisher.sink { completion in
            if case .failure = completion { XCTFail() }
        } receiveValue: { value in
            valueMutex.withLock { $0 = value }
        }
        XCTAssertTrue(valueMutex.withLock { $0 })
    }
}
