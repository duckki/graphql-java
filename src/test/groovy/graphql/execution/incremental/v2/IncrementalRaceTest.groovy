package graphql.execution.incremental.v2

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import graphql.Directives
import graphql.ExecutionInput
import graphql.ExperimentalApi
import graphql.GraphQL
import graphql.TestUtil
import graphql.execution.incremental.v2.fuzz.FuzzObservation
import graphql.schema.GraphQLSchema
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Timeout(30)
class IncrementalRaceTest extends Specification {
    GraphQL engine() {
        def sdl = 'type Query { user: User users: [User] } type User { id: ID! name: String values: [Int] }'
        def schema = GraphQLSchema.newSchema(TestUtil.schema(sdl)).additionalDirective(Directives.StreamDirective).build()
        GraphQL.newGraphQL(schema).build()
    }

    ExecutionInput input(String query, Map root) {
        ExecutionInput.newExecutionInput().query(query).root(root).graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()
    }

    def 'cancellation closes a held child source before its producer field settles'() {
        given:
        def name = new CompletableFuture()
        def source = new BarrierItems(values: [1, 2])
        def input = input('{ user { id ... @defer { values @stream name } } }', [user: [id: '1', name: name, values: source]])
        def result = engine().execute(input)
        def subscriber = new IncrementalTestSubscriber()
        result.incrementalItemPublisher.subscribe(subscriber)
        source.acquired.get(5, TimeUnit.SECONDS)

        expect:
        subscriber.results.empty
        source.pulled.get() == 0

        when:
        input.cancel()
        def error = subscriber.finished.handle { value, failure -> failure }.get(5, TimeUnit.SECONDS)
        name.complete('late')

        then:
        error != null
        source.closed.get() == 1
        source.pulled.get() == 0
        subscriber.results.empty
    }

    def 'barrier released cancellation and item completion close every acquired source once'() {
        given:
        def executor = Executors.newFixedThreadPool(2)
        def engine = engine()

        expect:
        (0..<100).every { iteration -> race(engine, executor, iteration); true }

        cleanup:
        executor.shutdownNow()
        executor.awaitTermination(5, TimeUnit.SECONDS)
    }

    void race(GraphQL engine, def executor, int iteration) {
        def item = new CompletableFuture()
        def second = new CompletableFuture()
        def name = new CompletableFuture()
        def child = new BarrierItems(values: [1, 2])
        def outer = new BarrierItems(values: [item, second])
        def input = input('{ users @stream { id ... @defer { name values @stream } } }', [users: outer])
        def result = engine.execute(input)
        def subscriber = new IncrementalTestSubscriber(initialDemand: iteration % 2 == 0 ? 1 : Long.MAX_VALUE)
        result.incrementalItemPublisher.subscribe(subscriber)
        outer.firstPull.get(5, TimeUnit.SECONDS)
        def release = new CountDownLatch(1)
        def cancel = CompletableFuture.runAsync({ assert release.await(5, TimeUnit.SECONDS); input.cancel() }, executor)
        def complete = CompletableFuture.runAsync({ assert release.await(5, TimeUnit.SECONDS); item.complete([id: '1', name: name, values: child]) }, executor)
        release.countDown()
        CompletableFuture.allOf(cancel, complete).get(5, TimeUnit.SECONDS)
        subscriber.finished.handle { value, failure -> failure }.get(5, TimeUnit.SECONDS)
        def before = new ArrayList<>(subscriber.results)
        name.complete('late')
        second.complete([id: '2', name: 'late', values: [3]])
        assert subscriber.results == before
        assert outer.closed.get() == 1
        assert child.closed.get() == (child.acquired.done ? 1 : 0)
        def json = new ObjectMapper()
        new FuzzObservation().reconstructPrefix(json.valueToTree(result.toSpecification()), (ArrayNode) json.valueToTree(before))
    }
}
