package graphql.execution.incremental.v2

import com.fasterxml.jackson.databind.ObjectMapper
import graphql.ExecutionInput
import graphql.ExperimentalApi
import graphql.ParseAndValidate
import graphql.execution.incremental.v2.fuzz.FuzzInputGenerator
import graphql.execution.incremental.v2.fuzz.FuzzObservation
import graphql.execution.incremental.v2.fuzz.FuzzSchema
import graphql.execution.incremental.v2.fuzz.FuzzProfile
import graphql.execution.incremental.v2.fuzz.FuzzScenarioGenerator
import graphql.execution.incremental.v2.fuzz.FuzzQueryRenderer
import graphql.execution.incremental.v2.fuzz.FuzzHostChecks
import graphql.execution.incremental.v2.fuzz.FuzzSemanticCoverage
import graphql.execution.incremental.v2.fuzz.FuzzInputCheck
import graphql.execution.incremental.v2.fuzz.FuzzShrinker
import graphql.execution.incremental.v2.fuzz.DifferentialMismatch
import graphql.execution.incremental.v2.fuzz.FuzzValidationChecks
import graphql.execution.incremental.v2.fuzz.FuzzFaultChecks
import graphql.execution.incremental.v2.fuzz.FuzzInput

import java.nio.file.Paths
import spock.lang.Specification
import spock.lang.Unroll

class IncrementalFuzzHarnessTest extends Specification {
    def json = new ObjectMapper()

    def 'type directed generator produces deterministic valid GraphQL with matching variables'() {
        given:
        def schema = new FuzzSchema('sync').graphQL.graphQLSchema

        expect:
        (0..255).every { seed ->
            def input = new FuzzInputGenerator().generate(seed)
            def repeated = new FuzzInputGenerator().generate(seed)
            def execution = ExecutionInput.newExecutionInput().query(input.query).variables(json.convertValue(input.variables, Map))
                    .graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()
            assert input.query == repeated.query
            assert input.root == repeated.root
            assert !ParseAndValidate.parseAndValidate(schema, execution).failure
            true
        }
    }

    def 'reconstruction resolves IDs to paths independently of their numeric values'() {
        given:
        def initial = json.readTree('{"data":{"values":[]},"pending":[{"id":"73","path":["values"]}],"errors":0,"hasNext":true}')
        def updates = json.readTree('[{"incremental":[{"id":"73","items":[null,2],"errors":1}],"completed":[{"id":"73","errors":0}],"hasNext":false}]')

        expect:
        new FuzzObservation().reconstruct(initial, updates) == json.readTree('{"data":{"values":[null,2]},"errors":1}')
    }

    def 'balanced scenarios and named fragment lowering stay deterministic and valid'() {
        given:
        def schema = new FuzzSchema('sync').schema
        def profile = new FuzzProfile()
        def generator = new FuzzScenarioGenerator(profile, schema)
        def families = [] as Set

        expect:
        (0..999).every { seed ->
            def input = generator.generate(seed)
            def repeated = generator.generate(seed)
            families.add(input.family)
            assert input.query == repeated.query
            assert input.root == repeated.root
            ['inline', 'named'].each { document ->
                def query = new FuzzQueryRenderer(schema).render(input.selections, document == 'named')
                def execution = ExecutionInput.newExecutionInput().query(query).variables(json.convertValue(input.variables, Map))
                        .graphQLContext([(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2): true]).build()
                def result = ParseAndValidate.parseAndValidate(schema, execution)
                assert !result.failure: result.validationErrors
            }
            true
        }
        families == FuzzScenarioGenerator.FAMILIES.toSet()
    }

    def 'host profiles close acquired sources and keep cancellation within the pull window'() {
        given:
        def schema = new FuzzSchema('async')
        def generator = new FuzzScenarioGenerator(new FuzzProfile(), schema.schema)
        def coverage = new FuzzSemanticCoverage(Paths.get('build/incremental-fuzz/unit'), false)
        def host = new FuzzHostChecks(schema, coverage)

        expect:
        (0..59).every { seed -> host.check(generator.generate(seed)); true }
        (0..5).every { coverage.bins.path('host.' + it).asInt() > 0 }
        coverage.bins.path('host.closed-source').asInt() > 0
        coverage.bins.path('host.second-subscriber').asInt() > 0

        cleanup:
        schema.close()
    }

    def 'shrinker retains reproduced semantic failures and preserves the generator family'() {
        given:
        def schema = new FuzzSchema('sync')
        def original = new FuzzScenarioGenerator(new FuzzProfile(), schema.schema).generate(22)
        FuzzInputCheck check = { input ->
            if (input.query.contains('name')) throw new DifferentialMismatch('reproduced')
        }

        when:
        def minimized = new FuzzShrinker(check).minimize(original)

        then:
        minimized.query.length() < original.query.length()
        minimized.query.contains('name')
        minimized.family == original.family
        minimized.root == original.root
    }

    def 'shrinker does not replace the original failure with a different failure category'() {
        given:
        def selections = json.readTree('[{"kind":"field","name":"a","responseName":"a","selections":[],"directives":[]},{"kind":"field","name":"b","responseName":"b","selections":[],"directives":[]}]')
        def original = new FuzzInput(1, selections, json.createObjectNode(), json.createObjectNode())
        FuzzInputCheck check = { input ->
            if (input.selections.size() == 2) throw new DifferentialMismatch('original category: detail')
            throw new DifferentialMismatch('different category: detail')
        }

        expect:
        new FuzzShrinker(check).minimize(original).selections == original.selections
    }

    def 'exceptional streamed items cover nullable and fatal prefix and tail outcomes'() {
        given:
        def schema = new FuzzSchema('async')
        def coverage = new FuzzSemanticCoverage(Paths.get('build/incremental-fuzz/unit'), false)
        def host = new FuzzHostChecks(schema, coverage)

        expect:
        [false, true].every { strict ->
            [0, 1].every { prefix -> host.checkRejectedItems(strict, prefix); true }
        }
        ['nullable', 'fatal'].every { kind ->
            [0, 1].every { prefix -> coverage.bins.path("host.rejected-item.$kind.$prefix").asInt() > 0 }
        }

        cleanup:
        schema.close()
    }

    def 'native validation profiles cover active conditions and stable negative diagnostics'() {
        given:
        def coverage = new FuzzSemanticCoverage(Paths.get('build/incremental-fuzz/unit'), false)
        def validator = new FuzzValidationChecks(coverage)

        expect:
        (0..127).every { seed -> validator.check(seed); true }
        coverage.bins.path('native.subscription-allowed').asInt() > 0
        coverage.bins.path('native.subscription-rejected').asInt() > 0
        (0..9).every { coverage.bins.path('native.validation-negative.' + it).asInt() > 0 }
    }

    def 'multi-failure completion selects its cause while ordinary responses preserve diagnostics'() {
        given:
        def schema = new FuzzSchema('sync')
        def input = FuzzInput.fromJson(json.readTree(new File('fuzz/incremental/native-corpus/causal-error-order.json')).input)
        def faults = new FuzzFaultChecks(schema, 'named', 'burst')

        when:
        faults.check(input)

        then:
        faults.observation.incremental.updates[0].completed[0].errors*.path == [json.readTree('["required"]')]
        faults.observation.ordinaryV2.initial.errors.size() == 3
        faults.observation.ordinaryExisting.initial.errors.size() == 3

        cleanup:
        schema.close()
    }

    def 'native multi-failure generator exercises origins and ordinary parity across resolver modes'() {
        given:
        def schemas = ['sync', 'promise', 'items', 'async'].collectEntries { [(it): new FuzzSchema(it)] }
        def generator = new FuzzScenarioGenerator(new FuzzProfile(), schemas.sync.schema)

        expect:
        (0..59).findAll { generator.generate(it).family == "structural" }.every { seed ->
            schemas.values().each { schema ->
                new FuzzFaultChecks(schema, seed % 2 == 0 ? 'inline' : 'named', 'one').check(generator.generateFaults(seed))
            }
            true
        }

        cleanup:
        schemas.values().each { it.close() }
    }

    def 'native error-origin observer rejects fabricated and duplicated errors'() {
        given:
        def schema = new FuzzSchema('sync')
        def input = FuzzInput.fromJson(json.readTree(new File('fuzz/incremental/native-corpus/causal-error-order.json')).input)
        def faults = new FuzzFaultChecks(schema, 'inline', 'one')
        def fabricated = json.readTree('{"initial":{"errors":[{"path":["user","name"],"extensions":{"classification":"DataFetchingException"}}]},"updates":[]}')
        def duplicate = json.readTree('{"initial":{"errors":[{"path":["a"],"extensions":{"classification":"DataFetchingException"}},{"path":["a"],"extensions":{"classification":"DataFetchingException"}}]},"updates":[]}')

        expect:
        [fabricated, duplicate].every { observation ->
            try { faults.checkOrigins(input, observation); assert false }
            catch (DifferentialMismatch expected) { true }
        }

        cleanup:
        schema.close()
    }

    @Unroll
    def 'lifecycle checker rejects #updates'() {
        given:
        def initial = json.readTree('{"data":{"values":[]},"pending":[{"id":"0","path":["values"]}],"hasNext":true}')

        when:
        new FuzzObservation().reconstruct(initial, json.readTree(updates))

        then:
        thrown(IllegalStateException)

        where:
        updates << [
                '[{"incremental":[{"id":"1","items":[2]}],"hasNext":false}]',
                '[{"completed":[{"id":"0"},{"id":"0"}],"hasNext":false}]',
                '[{"hasNext":false}]',
                '[{"completed":[{"id":"0"}],"hasNext":true}]'
        ]
    }
    @Unroll
    def 'initial diagnostics and failed delivery causes remain separate: #fixture'() {
        given:
        def input = FuzzInput.fromJson(json.readTree(new File('fuzz/incremental/corpus/' + fixture + '.json')).input)
        def schemas = ['sync', 'promise', 'items', 'async'].collectEntries { [(it): new FuzzSchema(it)] }

        expect:
        schemas.values().every { schema ->
            ['inline', 'named'].each { document ->
                def checks = new FuzzFaultChecks(schema, document, 'one')
                checks.check(input)
                def result = checks.observation.incremental
                assert result.initial.errors*.path == [json.readTree('["initial"]')]
                assert result.updates.collectMany { it.completed.toList() }.collectMany { it.errors.toList() }*.path == [json.valueToTree(cause)]
                assert checks.observation.ordinaryV2.initial.errors.size() == ordinaryErrors
            }
            true
        }

        cleanup:
        schemas.values().each { it.close() }

        where:
        fixture                       | cause                          | ordinaryErrors
        'initial-and-deferred-cause'   | ['required']                   | 4
        'initial-and-streamed-cause'   | ['strictUsers', 0, 'required']  | 3
    }

}
