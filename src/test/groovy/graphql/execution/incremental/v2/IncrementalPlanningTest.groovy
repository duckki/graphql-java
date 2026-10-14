package graphql.execution.incremental.v2

import graphql.GraphQLContext
import graphql.TestUtil
import graphql.execution.FieldCollectorParameters
import graphql.language.NodeUtil
import graphql.parser.Parser
import spock.lang.Specification

class IncrementalPlanningTest extends Specification {
    CollectedFields collect(String query) {
        def schema = TestUtil.schema('type Query { a: String b: String c: String }')
        def operation = NodeUtil.getOperation(Parser.parse(query), null)
        def parameters = FieldCollectorParameters.newParameters().schema(schema).objectType(schema.queryType)
                .fragments(operation.fragmentsByName).variables([:]).graphQLContext(GraphQLContext.getDefault()).build()
        new IncrementalFieldCollector().collectFields(parameters, operation.operationDefinition.selectionSet)
    }

    def 'immediate fields dominate overlap and ancestor owners dominate descendants'() {
        given:
        def fields = collect('''{ a ... @defer(label: "parent") { a b ... @defer(label: "child") { b c } }
            ... @defer(label: "sibling") { b } }''')
        def plan = new ExecutionPlan(fields, [] as Set)

        expect:
        plan.immediateFields.fields.keySet().toList() == ['a']
        plan.deferredFields.values()*.fields*.keySet()*.toList() == [['b'], ['c']]
        plan.deferredFields.keySet().collect { it*.label } == [['parent', 'sibling'], ['child']]
        fields.newDeferUsages*.label == ['parent', 'child', 'sibling']
    }

    def 'named fragments retain distinct directive identities under shared owners'() {
        given:
        def fields = collect('{ ...Info @defer(label: "left") ...Info @defer(label: "right") } fragment Info on Query { b }')
        def plan = new ExecutionPlan(fields, [] as Set)

        expect:
        fields.fields.b.size() == 2
        !fields.fields.b[0].deferUsage.is(fields.fields.b[1].deferUsage)
        plan.deferredFields.size() == 1
        plan.deferredFields.keySet().first()*.label == ['left', 'right']
    }

    def 'defer context overlays preserve their inherited bindings'() {
        given:
        def fields = collect('{ ... @defer(label: "left") { a } ... @defer(label: "right") { b } }')
        def left = new DeliveryGroup(null, graphql.execution.ResultPath.rootPath(), 'left')
        def right = new DeliveryGroup(null, graphql.execution.ResultPath.rootPath(), 'right')
        def bindings = [(fields.newDeferUsages[0]): left]
        def parent = new DeferContext(null, bindings)
        def child = new DeferContext(parent, [(fields.newDeferUsages[1]): right])
        bindings.clear()

        expect:
        child.get(fields.newDeferUsages[0]).is(left)
        child.get(fields.newDeferUsages[1]).is(right)
        parent.get(fields.newDeferUsages[1]) == null
    }
}
