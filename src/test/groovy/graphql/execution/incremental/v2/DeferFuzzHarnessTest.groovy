package graphql.execution.incremental.v2

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import graphql.execution.incremental.v2.fuzz.DeferInputGenerator
import graphql.execution.incremental.v2.fuzz.DifferentialMismatch
import graphql.execution.incremental.v2.fuzz.FuzzInput
import graphql.execution.incremental.v2.fuzz.FuzzObservation
import graphql.execution.incremental.v2.fuzz.FuzzSchema
import graphql.execution.incremental.v2.fuzz.FuzzShrinker
import graphql.execution.incremental.v2.fuzz.FuzzInputCheck
import spock.lang.Specification

class DeferFuzzHarnessTest extends Specification {
    def json = new ObjectMapper()

    def 'defer generation is deterministic and every generated document validates'() {
        given:
        def schema = new FuzzSchema('sync')
        def generator = new DeferInputGenerator(schema.schema)

        expect:
        (0..<120).every { seed ->
            def first = generator.generate(seed)
            def second = generator.generate(seed)
            assert first.query == second.query
            assert first.root == second.root
            assert !first.query.contains('@stream')
            assert schema.graphQL.execute(graphql.ExecutionInput.newExecutionInput().query(first.query).root(json.convertValue(first.root, Object))
                    .variables(json.convertValue(first.variables, Map)).graphQLContext([(graphql.ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()).dataPresent
            true
        }
    }

    def 'observer rejects patches for an unknown ID'() {
        when:
        new FuzzObservation().reconstruct(json.readTree('{"data":{},"pending":[{"id":"0","path":[]}],"hasNext":true}'),
                (ArrayNode) json.readTree('[{"incremental":[{"id":"unknown","data":{"a":1}}],"completed":[{"id":"0"}],"hasNext":false}]'))

        then:
        thrown(DifferentialMismatch)
    }

    def 'shrinking retains the same mismatch category'() {
        given:
        def schema = new FuzzSchema('sync')
        def input = new DeferInputGenerator(schema.schema).generate(2)
        FuzzInputCheck check = { FuzzInput candidate -> throw new DifferentialMismatch('test mismatch: ' + candidate.selections.size()) }

        when:
        def minimized = new FuzzShrinker(check).minimize(input)

        then:
        minimized.selections.size() == 1
        minimized.seed == input.seed
    }
}
