package graphql.execution.incremental.v2

import graphql.incremental.CompletedPayload
import graphql.incremental.IncrementalPayloadV2
import graphql.incremental.IncrementalExecutionResultV2
import graphql.incremental.PendingPayload
import spock.lang.Specification

class IncrementalPayloadV2Test extends Specification {
    def 'pending and completion notices take snapshots and support transforms'() {
        given:
        def path = ['users', 0]
        def pending = PendingPayload.newPendingPayload().id('0').path(path).build()
        path.add('changed')

        expect:
        pending.toSpecification() == [id: '0', path: ['users', 0]]
        pending.transform { it.label('item') }.toSpecification() == [id: '0', path: ['users', 0], label: 'item']
        CompletedPayload.newCompletedPayload().id('0').build().transform { it.id('1') }.toSpecification() == [id: '1']
    }

    def 'defer patches snapshot their inputs and copy through transform'() {
        given:
        def values = [x: null]
        def patch = IncrementalPayloadV2.newIncrementalPayloadV2().id('0').data(values).build()
        values.put('y', 3)

        expect:
        patch.toSpecification() == [id: '0', data: [x: null]]
        patch.transform { it.id('1') }.toSpecification() == [id: '1', data: [x: null]]
        IncrementalPayloadV2.newIncrementalPayloadV2().id('0').data([x: null]).build().toSpecification() == [id: '0', data: [x: null]]
    }

    def 'transformation of initial results preserves pending notices'() {
        given:
        def pending = PendingPayload.newPendingPayload().id('0').path([]).build()
        def result = IncrementalExecutionResultV2.newIncrementalExecutionResultV2().data([a: 1]).pending([pending]).incrementalItemPublisher({ subscriber -> } as org.reactivestreams.Publisher).build()

        expect:
        result.transform { it.data([a: 2]) }.toSpecification() == [data: [a: 2], pending: [[id: '0', path: []]], hasNext: true]
    }
}
