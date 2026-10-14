package graphql.execution.incremental.v2

import graphql.Directives
import graphql.ExecutionInput
import graphql.ExperimentalApi
import graphql.GraphQL
import graphql.TestUtil
import graphql.schema.GraphQLSchema
import graphql.schema.TypeResolver
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.TypeRuntimeWiring
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

@Timeout(10)
class IncrementalPolymorphismTest extends Specification {
    String sdl = '''
        interface Node { id: ID! }
        interface Named implements Node { id: ID! name: String }
        type User implements Named & Node { id: ID! name: String friend: Node values: [Int] }
        type Product implements Named & Node { id: ID! name: String }
        union Search = User | Product
        type Query { node: Named unionNode: Search nodes: [Named] search: [Search] }
        type Mutation { first: User second: User }
    '''

    GraphQL engine(RuntimeWiring.Builder wiring = RuntimeWiring.newRuntimeWiring()) {
        TypeResolver resolver = { env -> env.schema.getObjectType(env.object.__typename) }
        ['Node', 'Named', 'Search'].each { wiring.type(TypeRuntimeWiring.newTypeWiring(it).typeResolver(resolver)) }
        def schema = TestUtil.schema(sdl, wiring.build())
        GraphQL.newGraphQL(schema).build()
    }

    ExecutionInput input(String query, Map root) {
        ExecutionInput.newExecutionInput().query(query).root(root).graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()
    }

    List<Map> collect(def result) {
        def subscriber = new IncrementalTestSubscriber()
        result.incrementalItemPublisher.subscribe(subscriber)
        subscriber.awaitResults()
    }

    def 'interface and union conditions match inherited interfaces and filter concrete fragments'() {
        given:
        def root = [(field): [__typename: runtime, id: '1', name: 'name', friend: [__typename: 'Product', id: '2']]]
        def query = "{ ${field} { __typename ... on Node { id } ...Names @defer(label: \"names\") ... on User @defer(label: \"friend\") { friend { id } } } } fragment Names on Named { name }"

        when:
        def result = engine().execute(input(query, root))
        def updates = collect(result)

        then:
        result.errors.empty
        result.data == [(field): [__typename: runtime, id: '1']]
        result.pending*.label == (runtime == 'User' ? ['names', 'friend'] : ['names'])
        updates.collectMany { it.incremental ?: [] }*.data == (runtime == 'User' ? [[name: 'name'], [friend: [id: '2']]] : [[name: 'name']])

        where:
        field       | runtime
        'node'      | 'User'
        'node'      | 'Product'
        'unionNode' | 'User'
        'unionNode' | 'Product'
    }

    def 'mutation roots await ordinary completion serially while nested delivery stays lazy'() {
        given:
        def first = new CompletableFuture()
        def prefix = new CompletableFuture()
        def calls = new CopyOnWriteArrayList<String>()
        def wiring = RuntimeWiring.newRuntimeWiring().type(TypeRuntimeWiring.newTypeWiring('Mutation')
                .dataFetcher('first', { calls.add('first'); first })
                .dataFetcher('second', { calls.add('second'); [__typename: 'User', id: '2', name: 'Grace'] }))
                .type(TypeRuntimeWiring.newTypeWiring('User').dataFetcher('name', { env -> calls.add('name'); env.source.name }))
        def query = 'mutation { first { id values ... @defer { name } } second { id ... @defer { name } } }'
        def initial = engine(wiring).executeAsync(input(query, [:]))

        expect:
        calls == ['first']

        when:
        first.complete([__typename: 'User', id: '1', name: 'Ada', values: prefix])

        then:
        !initial.done
        calls == ['first']

        when:
        prefix.complete([1, 2])
        def result = initial.get(5, TimeUnit.SECONDS)

        then:
        calls == ['first', 'second']
        result.data == [first: [id: '1', values: [1, 2]], second: [id: '2']]

        when:
        def updates = collect(result)

        then:
        calls.count('name') == 2
        updates.collectMany { it.incremental ?: [] }*.data == [[name: 'Ada'], [name: 'Grace']]
    }
}
