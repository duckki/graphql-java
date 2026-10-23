package graphql.execution.incremental.v2

import graphql.GraphqlErrorBuilder
import graphql.execution.ResultPath
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

@Timeout(10)
class IncrementalCachedFailureTest extends Specification {
    ResultPath path = ResultPath.rootPath()

    WorkResult success(Map value) {
        new WorkResult(value, [], new IncrementalWork(), false)
    }

    WorkResult failure(int count) {
        def errors = (0..<count).collect { GraphqlErrorBuilder.newError().message('cause ' + it).build() }
        new WorkResult(null, errors, new IncrementalWork(), true)
    }

    DeferredTask task(List<DeliveryGroup> owners, CompletableFuture<WorkResult> result, IncrementalWork work = new IncrementalWork()) {
        new DeferredTask(owners, path, work, { result })
    }

    def 'initial cached rejection skips private work only when already available: #synchronous'() {
        given:
        def left = new DeliveryGroup(null, path, 'left')
        def right = new DeliveryGroup(null, path, 'right')
        def rejected = new CompletableFuture<WorkResult>()
        if (synchronous) rejected.complete(failure(1))
        def starts = new AtomicInteger()
        def rejectedWork = new IncrementalWork()
        def shared = task([left, right], rejected, rejectedWork)
        def privateTask = new DeferredTask([right], path, new IncrementalWork(), {
            starts.incrementAndGet()
            CompletableFuture.completedFuture(success([private: true]))
        })
        def work = new IncrementalWork()
        [left, right].each { work.addGroup(it) }
        [shared, privateTask].each { work.addTask(it) }
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()

        when:
        queue.publisher.subscribe(subscriber)
        if (!synchronous) rejected.complete(failure(1))
        def updates = subscriber.awaitResults()

        then:
        starts.get() == (synchronous ? 0 : 1)
        rejectedWork.checkRunning() == null
        updates.collectMany { it.completed ?: [] }*.id == ['0', '1']
        updates.collectMany { it.completed ?: [] }*.errors*.size() == [1, 1]
        updates.collectMany { it.incremental ?: [] }.empty
        !updates.last().hasNext

        where:
        synchronous << [true, false]
    }

    def 'startup aggregates all available causes and keeps its queued notice after root removal'() {
        given:
        def parent = new DeliveryGroup(null, path, 'parent')
        def child = new DeliveryGroup(parent, path, 'child')
        def left = new DeliveryGroup(null, path, 'left')
        def right = new DeliveryGroup(null, path, 'right')
        def parentResult = new CompletableFuture<WorkResult>()
        def first = new CompletableFuture<WorkResult>()
        def second = new CompletableFuture<WorkResult>()
        def starts = new AtomicInteger()
        def work = new IncrementalWork()
        [parent, child, left, right].each { work.addGroup(it) }
        work.addTask(task([parent], parentResult))
        work.addTask(task([left, child], first))
        work.addTask(task([right, child], second))
        work.addTask(new DeferredTask([child], path, new IncrementalWork(), {
            starts.incrementAndGet()
            CompletableFuture.completedFuture(success([private: true]))
        }))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber(initialDemand: 0)
        queue.publisher.subscribe(subscriber)

        when:
        parentResult.complete(success([parent: true]))
        first.complete(failure(2))
        second.complete(failure(3))
        subscriber.subscription.request(1)

        then:
        starts.get() == 0
        subscriber.results.size() == 1
        subscriber.results[0].completed*.id == ['0', '1', '2']
        subscriber.results[0].completed.findAll { it.errors }*.errors*.size() == [2, 3]
        subscriber.results[0].pending == [[id: '3', path: [], label: 'child']]
        subscriber.results[0].hasNext
        !queue.stopped

        when:
        subscriber.subscription.request(1)
        def updates = subscriber.awaitResults()

        then:
        updates[1].completed*.id == ['3']
        updates[1].completed[0].errors.size() == 5
        !updates[1].hasNext
    }

    def 'raw rejected computation is cached and never reevaluated or cancelled'() {
        given:
        def starts = new AtomicInteger()
        def work = new IncrementalWork()
        def task = new DeferredTask([], path, work, { starts.incrementAndGet(); throw new IllegalStateException('boom') })

        when:
        def first = task.result()
        task.abort()

        then:
        first.isCompletedExceptionally()
        task.result().is(first)
        task.rejection[0].message.contains('boom')
        starts.get() == 1
        work.checkRunning() == null
    }

    def 'duplicate task references increment owner pending only once'() {
        given:
        def owner = new DeliveryGroup(null, path, null)
        def computation = task([owner], CompletableFuture.completedFuture(success([value: 1])))
        def work = new IncrementalWork()
        work.addGroup(owner)
        work.addTask(computation)
        work.addTask(computation)
        def subscriber = new IncrementalTestSubscriber()

        when:
        new IncrementalWorkQueue(work, {}).publisher.subscribe(subscriber)

        then:
        subscriber.awaitResults() == [[incremental: [[id: '0', data: [value: 1]]], completed: [[id: '0']], hasNext: false]]
    }

    def 'an absent undeclared parent never promotes its late child to a root'() {
        given:
        def missing = new DeliveryGroup(null, path, 'missing')
        def child = new DeliveryGroup(missing, path, 'child')
        def starts = new AtomicInteger()
        def work = new IncrementalWork()
        work.addGroup(child)
        work.addTask(new DeferredTask([child], path, new IncrementalWork(), {
            starts.incrementAndGet()
            CompletableFuture.completedFuture(success([bad: true]))
        }))

        when:
        def queue = new IncrementalWorkQueue(work, {})

        then:
        queue.initialPending.empty
        starts.get() == 0
    }

    def 'child-first declarations prune a twenty-thousand-level forest iteratively'() {
        given:
        def groups = []
        20000.times { groups.add(new DeliveryGroup(groups ? groups.last() : null, path, null)) }
        def work = new IncrementalWork()
        groups.reverseEach { work.addGroup(it) }
        work.addTask(task([groups.last()], CompletableFuture.completedFuture(success([leaf: true]))))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()

        expect:
        queue.initialPending.size() == 1

        when:
        queue.publisher.subscribe(subscriber)

        then:
        subscriber.awaitResults() == [[incremental: [[id: '0', data: [leaf: true]]], completed: [[id: '0']], hasNext: false]]
    }

    def 'delivery projections preserve collected diagnostics and explicit multi-error causes'() {
        given:
        def diagnostics = failure(4).errors
        def cause = diagnostics.take(2)
        def outcome = new WorkResult(null, diagnostics, new IncrementalWork(), true, cause)

        expect:
        outcome.errors.size() == 4
        outcome.deliveryErrors.size() == 2
        outcome.transform { it.value([caught: null]).failed(false) }.deliveryErrors.size() == 4
        outcome.transform { it.cause([]) }.deliveryErrors.empty
    }

    def 'a queued failure keeps its deeper wire ID available until FIFO notification'() {
        given:
        def parent = new DeliveryGroup(null, path, 'parent')
        def child = new DeliveryGroup(parent, path.segment('nested'), 'child')
        def supporter = new DeliveryGroup(null, path, 'supporter')
        def failureOwner = new DeliveryGroup(null, path, 'failure')
        def parentResult = new CompletableFuture<WorkResult>()
        def value = new CompletableFuture<WorkResult>()
        def rejected = new CompletableFuture<WorkResult>()
        def work = new IncrementalWork()
        [parent, child, supporter, failureOwner].each { work.addGroup(it) }
        work.addTask(task([parent], parentResult))
        work.addTask(new DeferredTask([supporter, child], path.segment('nested'), new IncrementalWork(), { value }))
        work.addTask(task([failureOwner, child], rejected))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber(initialDemand: 0)
        queue.publisher.subscribe(subscriber)

        when:
        parentResult.complete(success([parent: true]))
        value.complete(success([value: 1]))
        rejected.complete(failure(1))
        subscriber.subscription.request(1)

        then:
        subscriber.results[0].incremental.find { it.data.containsKey('value') } == [id: '3', data: [value: 1]]
        subscriber.results[0].completed*.id == ['0', '1', '2']
        subscriber.results[0].hasNext

        when:
        subscriber.subscription.request(1)

        then:
        subscriber.awaitResults()[1].completed*.id == ['3']
        !subscriber.results.last().hasNext
    }

    def 'zero-error rejected cache still prevents private evaluation'() {
        given:
        def left = new DeliveryGroup(null, path, 'left')
        def right = new DeliveryGroup(null, path, 'right')
        def starts = new AtomicInteger()
        def work = new IncrementalWork()
        [left, right].each { work.addGroup(it) }
        work.addTask(task([left, right], CompletableFuture.completedFuture(failure(0))))
        work.addTask(new DeferredTask([right], path, new IncrementalWork(), {
            starts.incrementAndGet()
            CompletableFuture.completedFuture(success([private: true]))
        }))
        def subscriber = new IncrementalTestSubscriber()

        when:
        new IncrementalWorkQueue(work, {}).publisher.subscribe(subscriber)

        then:
        subscriber.awaitResults().collectMany { it.completed ?: [] }*.id == ['0', '1']
        starts.get() == 0
    }

    def 'cancellation aborts dormant work beneath twenty-thousand parent links without evaluation'() {
        given:
        def groups = []
        20000.times { groups.add(new DeliveryGroup(groups ? groups.last() : null, path, null)) }
        def starts = new AtomicInteger()
        def tasks = [groups.first(), groups.last()].collect { group ->
            new DeferredTask([group], path, new IncrementalWork(), {
                starts.incrementAndGet()
                CompletableFuture.completedFuture(success([bad: true]))
            })
        }
        def work = new IncrementalWork()
        groups.each { work.addGroup(it) }
        tasks.each { work.addTask(it) }
        def queue = new IncrementalWorkQueue(work, {})

        when:
        queue.cancel()

        then:
        tasks.every { it.result().isCompletedExceptionally() }
        starts.get() == 0
    }

    def 'failure removes twenty-thousand dormant descendants without evaluating them'() {
        given:
        def groups = []
        20000.times { groups.add(new DeliveryGroup(groups ? groups.last() : null, path, null)) }
        def starts = new AtomicInteger()
        def work = new IncrementalWork()
        groups.each { work.addGroup(it) }
        work.addTask(task([groups.first()], CompletableFuture.completedFuture(failure(1))))
        work.addTask(new DeferredTask([groups.last()], path, new IncrementalWork(), {
            starts.incrementAndGet()
            CompletableFuture.completedFuture(success([bad: true]))
        }))
        def subscriber = new IncrementalTestSubscriber()

        when:
        new IncrementalWorkQueue(work, {}).publisher.subscribe(subscriber)

        then:
        subscriber.awaitResults().collectMany { it.completed ?: [] }*.id == ['0']
        !subscriber.results.last().hasNext
        starts.get() == 0
    }

}
