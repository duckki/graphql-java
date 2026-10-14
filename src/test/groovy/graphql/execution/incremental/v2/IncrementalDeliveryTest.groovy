package graphql.execution.incremental.v2

import graphql.Directives
import graphql.ExecutionInput
import graphql.ExperimentalApi
import graphql.GraphQL
import graphql.TestUtil
import graphql.execution.DataFetcherResult
import graphql.execution.pubsub.ReactiveStreamsObjectPublisher
import graphql.execution.pubsub.CapturingSubscriber
import graphql.incremental.IncrementalExecutionResult
import graphql.schema.GraphQLSchema
import graphql.schema.DataFetcher
import org.dataloader.DataLoaderFactory
import org.dataloader.DataLoaderRegistry
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

@Timeout(10)
class IncrementalDeliveryTest extends Specification {
    String sdl = '''
        type Query { user: User users: [User] values: [Int] strict: [Int!] matrix: [[Int]] hello: String }
        type User { id: ID! name: String required: String! friend: User values: [Int] }
        type Mutation { update: User }
        type Subscription { event: User }
    '''

    GraphQL graphQL(Map fetchers = [:]) {
        def adapted = fetchers.collectEntries { type, fields -> [(type): fields.collectEntries { name, fetcher -> [(name): fetcher as DataFetcher] }] }
        def schema = TestUtil.schema(sdl, adapted)
        GraphQL.newGraphQL(schema).build()
    }

    def execute(String query, Object root = [:], Map fetchers = [:], Map variables = [:], Map options = [(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]) {
        graphQL(fetchers).execute(ExecutionInput.newExecutionInput().query(query).root(root).variables(variables).graphQLContext(options).build())
    }

    List<Map> collect(IncrementalExecutionResult result) {
        def subscriber = new IncrementalTestSubscriber()
        result.incrementalItemPublisher.subscribe(subscriber)
        subscriber.awaitResults()
    }

    def 'new option emits pending ids, patches and completion notices'() {
        when:
        def result = execute('{ user { id ... @defer(label: "details") { name } } }', [user: [id: '1', name: 'Ada']])
        def updates = collect(result)

        then:
        result.toSpecification() == [data: [user: [id: '1']], pending: [[id: '0', path: ['user'], label: 'details']], hasNext: true]
        updates == [[incremental: [[id: '0', data: [name: 'Ada']]], completed: [[id: '0']], hasNext: false]]
    }

    def 'legacy option retains path and label payloads'() {
        when:
        def result = execute('{ user { id ... @defer(label: "details") { name } } }', [user: [id: '1', name: 'Ada']], [:], [:], [(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT): true])
        def updates = collect(result)

        then:
        !result.toSpecification().containsKey('pending')
        updates == [[incremental: [[path: ['user'], label: 'details', data: [name: 'Ada']]], hasNext: false]]
    }

    @Unroll
    def 'inactive or empty defer gives an ordinary response: #query'() {
        expect:
        def result = execute(query, [user: [id: '1', name: 'Ada']])
        !(result instanceof IncrementalExecutionResult)
        result.toSpecification() == [data: data]

        where:
        query                                                              | data
        '{ user { name ... @defer(if: false) { id } } }'                    | [user: [id: '1', name: 'Ada']]
        '{ user { id ... @defer { name @skip(if: true) } } }'                | [user: [id: '1']]
        '{ user { name ... @defer { name } } }'                             | [user: [name: 'Ada']]
    }

    def 'nested defer releases child after parent, including fragments without directives'() {
        when:
        def result = execute('''{ user { id ... @defer(label: "outer") {
            ... { name ... @defer(label: "inner") { friend { id } } }
        } } }''', [user: [id: '1', name: 'Ada', friend: [id: '2']]])
        def updates = collect(result)

        then:
        result.pending*.label == ['outer']
        updates[0] == [incremental: [[id: '0', data: [name: 'Ada']]], completed: [[id: '0']], pending: [[id: '1', path: ['user'], label: 'inner']], hasNext: true]
        updates[1] == [incremental: [[id: '1', data: [friend: [id: '2']]]], completed: [[id: '1']], hasNext: false]
    }

    def 'shared field resolves once and contributes once across owners'() {
        given:
        def calls = new AtomicInteger()
        when:
        def result = execute('{ user { ... @defer(label: "a") { name } ... @defer(label: "b") { name id } } }',
                [user: [id: '1']], [User: [name: { calls.incrementAndGet(); 'Ada' }]])
        def updates = collect(result)

        then:
        calls.get() == 1
        updates.collectMany { it.incremental ?: [] }.findAll { it.data?.containsKey('name') }.size() == 1
        updates.collectMany { it.completed ?: [] }*.id == ['0', '1']
        updates.last().hasNext == false
    }

    def 'immediate overlap resolves once and defers only additional subfields'() {
        given:
        def calls = new AtomicInteger()
        when:
        def result = execute('{ user { friend { id } ... @defer { friend { name } } } }', [user: [:]],
                [User: [friend: { calls.incrementAndGet(); [id: '2', name: 'Grace'] }]])
        def updates = collect(result)

        then:
        calls.get() == 1
        result.data == [user: [friend: [id: '2']]]
        updates.collectMany { it.incremental ?: [] } == [[id: '0', subPath: ['friend'], data: [name: 'Grace']]]
    }

    def 'named fragments can be deferred under multiple owners'() {
        when:
        def result = execute('''{ user { ...Info @defer(label: "a") ...Info @defer(label: "b") } }
            fragment Info on User { name }''', [user: [name: 'Ada']])
        def updates = collect(result)

        then:
        result.pending*.label == ['a', 'b']
        updates.collectMany { it.incremental ?: [] }.size() == 1
        updates.collectMany { it.completed ?: [] }*.id == ['0', '1']
    }

    def 'fatal defer errors go on completed and suppress the failed patch and descendants'() {
        when:
        def result = execute('{ user { id ... @defer { required ... @defer { name } } } }', [user: [id: '1']])
        def updates = collect(result)

        then:
        updates.size() == 1
        !updates[0].containsKey('incremental')
        updates[0].completed[0].errors[0].path == ['user', 'required']
        updates[0].hasNext == false
    }

    def 'nullable errors stay in defer patches'() {
        when:
        def result = execute('{ user { id ... @defer { name } } }', [user: [id: '1']], [User: [name: { throw new IllegalStateException('boom') }]])
        def updates = collect(result)

        then:
        updates[0].incremental[0].data == [name: null]
        updates[0].incremental[0].errors[0].path == ['user', 'name']
        updates[0].completed == [[id: '0']]
    }

    def 'ordinary null bubbling rejects work beneath a nulled parent'() {
        when:
        def result = execute('{ user { required ... @defer { name } } }', [user: [:]])

        then:
        !(result instanceof IncrementalExecutionResult)
        result.data == [user: null]
    }

    def 'defer list occurrences keep their indices and distinct ids'() {
        when:
        def result = execute('{ users { id ... @defer(label: "details") { name } } }', [users: [[id: '1', name: 'Ada'], [id: '2', name: 'Grace']]])
        def updates = collect(result)

        then:
        result.pending*.path == [['users', 0], ['users', 1]]
        updates.collectMany { it.incremental ?: [] }*.data == [[name: 'Ada'], [name: 'Grace']]
    }

    @Unroll
    def 'validation rejects #query'() {
        expect:
        execute(query, [:], [:], variables).errors

        where:
        query                                                                        | variables
        'mutation { update { id } ... @defer { update { name } } }'                   | [:]
        'query($label: String) { user { ... @defer(label: $label) { name } } }'        | [label: 'x']
        '{ user { ... @defer(label: "x") { id } ... @defer(label: "x") { name } } }' | [:]
        'subscription { event { ... @defer { name } } }'                             | [:]
    }
    def 'data loader resolves deferred object fields'() {
        given:
        def loader = DataLoaderFactory.newDataLoader({ keys -> CompletableFuture.completedFuture(keys.collect { 'Name' + it }) })
        def registry = new DataLoaderRegistry().register('names', loader)
        def engine = graphQL([User: [name: { env -> env.getDataLoader('names').load(env.source.id) }]])
        def input = ExecutionInput.newExecutionInput().query('{ users { id ... @defer { name } } }')
                .root([users: [[id: '1'], [id: '2']]]).dataLoaderRegistry(registry)
                .graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()

        when:
        def updates = collect(engine.execute(input))

        then:
        updates.collectMany { it.incremental ?: [] }.findAll { it.data }*.data == [[name: 'Name1'], [name: 'Name2']]
        !updates.last().hasNext
    }

    def 'execution input cancellation closes incremental delivery and future subscriptions observe the error'() {
        given:
        def engine = graphQL()
        def input = ExecutionInput.newExecutionInput().query('{ user { id ... @defer { name } } }').root([user: [id: '1', name: 'Ada']])
                .graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()
        def result = engine.execute(input)
        def subscriber = new IncrementalTestSubscriber()

        when:
        input.cancel()
        result.incrementalItemPublisher.subscribe(subscriber)
        subscriber.awaitResults()

        then:
        def exception = thrown(java.util.concurrent.ExecutionException)
        exception.cause.message.contains('cancelled')
        subscriber.results.empty
    }

    def 'subscription conditions are checked after coercion and disabled delivery stays ordinary'() {
        given:
        def root = [event: new ReactiveStreamsObjectPublisher(1, { [name: 'Ada'] })]
        def query = 'subscription($enabled: Boolean!) { event { ... @defer(if: $enabled) { name } } }'

        when:
        def rejected = execute(query, root, [:], [enabled: true])
        def allowed = execute(query, root, [:], [enabled: false])
        def subscriber = new CapturingSubscriber()
        allowed.data.subscribe(subscriber)
        new PollingConditions(timeout: 5).eventually { assert subscriber.done.get() }

        then:
        rejected.errors*.message == ['The @defer directive must be disabled in subscription operations']
        subscriber.throwable == null
        subscriber.completed
        subscriber.events*.data == [[event: [name: 'Ada']]]
    }

    def 'a skipped subscription selection does not activate incremental delivery'() {
        given:
        def root = [event: new ReactiveStreamsObjectPublisher(1, { [name: 'Ada'] })]

        when:
        def result = execute('subscription { event { name ... @skip(if: true) { ... @defer { required } } } }', root)
        def subscriber = new CapturingSubscriber()
        result.data.subscribe(subscriber)
        new PollingConditions(timeout: 5).eventually { assert subscriber.done.get() }

        then:
        result.errors.empty
        subscriber.throwable == null
        subscriber.completed
        subscriber.events*.data == [[event: [name: 'Ada']]]
    }

    def 'named fragment visits retain inherited owners across shared composite selections'() {
        given:
        def name = new CompletableFuture()
        def calls = new AtomicInteger()
        def result = execute('''{
            ... @defer(label: "left") { user { ...Info } }
            ... @defer(label: "right") { user { ...Info } }
        } fragment Info on User { name }''', [user: [:]], [User: [name: { calls.incrementAndGet(); name }]])
        def subscriber = new IncrementalTestSubscriber()
        result.incrementalItemPublisher.subscribe(subscriber)

        expect:
        subscriber.results.empty

        when:
        name.complete('Ada')
        def updates = subscriber.awaitResults()

        then:
        calls.get() == 1
        updates.collectMany { it.incremental ?: [] } == [[id: '0', data: [user: [name: 'Ada']]]]
        updates.collectMany { it.completed ?: [] }*.id == ['0', '1']
    }

    def 'immediate named fragments dominate deferred visits without redundant notices'() {
        when:
        def result = execute('''{ user { ... @defer { ...Info } ...Info } }
            fragment Info on User { name }''', [user: [name: 'Ada']])

        then:
        !(result instanceof IncrementalExecutionResult)
        result.data == [user: [name: 'Ada']]
    }

    def 'explicit null labels are preserved on defer notices'() {
        when:
        def result = execute('{ user { ... @defer(label: null) { name } } }',
                [user: [name: 'Ada']])
        collect(result)

        then:
        result.toSpecification().pending.every { it.containsKey('label') && it.label == null }
        result.pending.every { it.hasLabel() }
    }

    def 'concurrent originating errors are retained once across a failed deferred scope'() {
        given:
        def selections = (0..<100).collect { "required${it}: required" }.join(' ')
        def result = execute("{ user { ... @defer { ${selections} } } }", [user: [:]],
                [User: [required: { CompletableFuture.supplyAsync { throw new IllegalStateException('failure') } }]])

        when:
        def updates = collect(result)

        then:
        updates.last().completed[0].errors.size() == 100
        updates.last().completed[0].errors*.path.toSet().size() == 100
        !updates.last().hasNext
    }

    def 'cancellation suppresses a late deferred resolver and its child notices'() {
        given:
        def name = new CompletableFuture()
        def result = execute('{ user { id ... @defer { name ... @defer { friend { id } } } } }',
                [user: [id: '1', friend: [id: '2']]], [User: [name: { name }]])
        def subscriber = new IncrementalTestSubscriber()
        result.incrementalItemPublisher.subscribe(subscriber)

        expect:
        subscriber.results.empty

        when:
        subscriber.subscription.cancel()
        name.complete('Ada')

        then:
        subscriber.results.empty
    }

}
