package graphql.execution.incremental.v2

import graphql.Directives
import graphql.ExecutionInput
import graphql.ExperimentalApi
import graphql.GraphQL
import graphql.TestUtil
import graphql.execution.pubsub.ReactiveStreamsObjectPublisher
import graphql.incremental.IncrementalExecutionResult
import graphql.schema.GraphQLSchema
import spock.lang.Specification
import spock.lang.Timeout

@Timeout(10)
class IncrementalTransitionTest extends Specification {
    String sdl = 'type Query { user: User hello: String } type User { id: ID! name: String } type Subscription { event: User }'

    GraphQL engine(IncrementalPreparsedCache cache) {
        def schema = GraphQLSchema.newSchema(TestUtil.schema(sdl)).additionalDirective(Directives.StreamDirective).build()
        GraphQL.newGraphQL(schema).preparsedDocumentProvider(cache).build()
    }

    def execute(GraphQL engine, String query, boolean legacy, boolean modern, Map variables = [:]) {
        engine.execute(ExecutionInput.newExecutionInput().query(query).variables(variables)
                .root([user: [id: '1', name: 'Ada'], hello: 'hello', event: new ReactiveStreamsObjectPublisher(1, { [id: '1', name: 'Ada'] })])
                .graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT): legacy, (ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): modern]).build())
    }

    def 'a warm parsed document follows flag transitions and V2 takes precedence'() {
        given:
        def cache = new IncrementalPreparsedCache(partitionValidation: false)
        def engine = engine(cache)
        def query = '{ user { id ... @defer(label: "details") { name } } }'

        expect:
        [[false, false], [true, false], [false, true], [true, true], [true, false], [false, false]].every { flags ->
            assertProtocol(execute(engine, query, flags[0], flags[1]), flags[0], flags[1])
            true
        }
        cache.computations.get() == 1
    }

    void assertProtocol(def result, boolean legacy, boolean modern) {
        assert result.errors.empty
        if (!legacy && !modern) {
            assert !(result instanceof IncrementalExecutionResult)
            assert result.toSpecification() == [data: [user: [id: '1', name: 'Ada']]]
            return
        }
        def subscriber = new IncrementalTestSubscriber()
        result.incrementalItemPublisher.subscribe(subscriber)
        def updates = subscriber.awaitResults()
        assert result.data == [user: [id: '1']]
        assert result.pending.size() == (modern ? 1 : 0)
        assert updates == (modern ? [[incremental: [[id: '0', data: [name: 'Ada']]], completed: [[id: '0']], hasNext: false]]
                : [[incremental: [[path: ['user'], label: 'details', data: [name: 'Ada']]], hasNext: false]])
    }

    def 'validation cache partitions the new flag in both warming orders'() {
        given:
        def cache = new IncrementalPreparsedCache()
        def engine = engine(cache)
        def query = 'query($label: String) { user { ... @defer(label: $label) { name } } }'

        expect:
        enabled.each { modern ->
            def result = execute(engine, query, false, modern, [label: 'details'])
            assert result.errors.empty == !modern
            if (!modern) assert result.data == [user: [name: 'Ada']]
        }
        cache.computations.get() == 2
        cache.entries.size() == 2

        where:
        enabled << [[false, true, false, true], [true, false, true, false]]
    }

    def 'cached subscription directives are evaluated with the current coerced variables'() {
        given:
        def cache = new IncrementalPreparsedCache()
        def engine = engine(cache)
        def query = 'subscription($enabled: Boolean!) { event { ... @defer(if: $enabled) { name } } }'

        expect:
        [false, true, false, true].every { enabled ->
            def result = execute(engine, query, false, true, [enabled: enabled])
            assert result.errors.empty == !enabled
            if (enabled) assert result.errors[0].message.contains('must be disabled')
            true
        }
        cache.computations.get() == 1
    }
    def 'stream validation cache partitions the new flag in both warming orders'() {
        given:
        def cache = new IncrementalPreparsedCache()
        def engine = engine(cache)
        def query = '{ hello @stream }'

        expect:
        enabled.each { modern ->
            def result = execute(engine, query, false, modern)
            assert result.errors.empty == !modern
            if (!modern) assert result.data == [hello: 'hello']
        }
        cache.computations.get() == 2
        cache.entries.size() == 2

        where:
        enabled << [[false, true, false, true], [true, false, true, false]]
    }


}
