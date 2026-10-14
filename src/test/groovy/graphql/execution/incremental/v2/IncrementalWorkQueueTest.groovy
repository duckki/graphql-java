package graphql.execution.incremental.v2

import graphql.GraphqlErrorBuilder
import graphql.execution.ResultPath
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class IncrementalWorkQueueTest extends Specification {
    ResultPath path = ResultPath.rootPath()

    DeferredTask task(List<DeliveryGroup> owners, CompletableFuture<WorkResult> future, IncrementalWork work = new IncrementalWork()) {
        new DeferredTask(owners, path, work, { future })
    }

    def success(Map data, IncrementalWork children = new IncrementalWork()) {
        new WorkResult(data, [], children, false)
    }

    def failure(String message) {
        new WorkResult(null, [GraphqlErrorBuilder.newError().message(message).build()], new IncrementalWork(), true)
    }

    def 'child failure settled under a shared owner waits for its parent notice'() {
        given:
        def parent = new DeliveryGroup(null, path, 'parent')
        def child = new DeliveryGroup(parent, path, 'child')
        def independent = new DeliveryGroup(null, path, 'independent')
        def parentResult = new CompletableFuture<WorkResult>()
        def sharedResult = new CompletableFuture<WorkResult>()
        def work = new IncrementalWork()
        [parent, child, independent].each { work.addGroup(it) }
        work.addTask(task([parent], parentResult))
        work.addTask(task([child, independent], sharedResult))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()
        queue.publisher.subscribe(subscriber)

        when:
        sharedResult.complete(failure('shared failure'))

        then:
        subscriber.results == [[completed: [[id: '1', errors: [[message: 'shared failure', locations: [], extensions: [classification: 'DataFetchingException']]]]], hasNext: true]]

        when:
        parentResult.complete(success([name: 'parent']))
        def results = subscriber.awaitResults()

        then:
        results[1].pending == [[id: '2', path: [], label: 'child']]
        results[1].completed*.id == ['0', '2']
        results[1].completed[1].errors*.message == ['shared failure']
        !results[1].hasNext
    }

    def 'failed but live dormant owner protects a shared task until last owner is removed'() {
        given:
        def parent = new DeliveryGroup(null, path, 'parent')
        def dormant = new DeliveryGroup(parent, path, 'dormant')
        def live = new DeliveryGroup(null, path, 'live')
        def parentResult = new CompletableFuture<WorkResult>()
        def liveFailure = new CompletableFuture<WorkResult>()
        def dormantFailure = new CompletableFuture<WorkResult>()
        def sharedResult = new CompletableFuture<WorkResult>()
        def held = new IncrementalWork()
        def work = new IncrementalWork()
        [parent, dormant, live].each { work.addGroup(it) }
        work.addTask(task([parent], parentResult))
        work.addTask(task([dormant, live], dormantFailure))
        work.addTask(task([dormant, live], sharedResult, held))
        work.addTask(task([live], liveFailure))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()
        queue.publisher.subscribe(subscriber)

        when:
        dormantFailure.complete(failure('cached'))

        then:
        held.checkRunning() == null

        when:
        parentResult.complete(success([name: 'parent']))
        subscriber.awaitResults()
        held.checkRunning()

        then:
        thrown(java.util.concurrent.CancellationException)

        when:
        sharedResult.complete(success([ignored: true], held))

        then:
        subscriber.results.collectMany { it.incremental ?: [] }.every { !it.data.containsKey('ignored') }
    }

    def 'rejected computations stay lazy and close does not notify twice'() {
        given:
        def starts = new AtomicInteger()
        def finishes = new AtomicInteger()
        def rejected = new DeferredTask([new DeliveryGroup(null, path, null)], path, new IncrementalWork(), {
            starts.incrementAndGet()
            CompletableFuture.completedFuture(success([bad: true]))
        })
        def work = new IncrementalWork()
        work.addTask(rejected)
        def queue = new IncrementalWorkQueue(work, { finishes.incrementAndGet() })
        def subscriber = new IncrementalTestSubscriber()

        when:
        queue.publisher.subscribe(subscriber)
        subscriber.awaitResults()
        subscriber.subscription.cancel()
        subscriber.subscription.cancel()

        then:
        starts.get() == 0
        finishes.get() == 1
    }

    def 'ready descendants retain release order and empty ancestors are pruned'() {
        given:
        def empty = new DeliveryGroup(null, path, 'empty')
        def first = new DeliveryGroup(empty, path, 'first')
        def second = new DeliveryGroup(empty, path, 'second')
        def work = new IncrementalWork()
        [empty, first, second].each { work.addGroup(it) }
        work.addTask(task([first], CompletableFuture.completedFuture(success([first: 1]))))
        work.addTask(task([second], CompletableFuture.completedFuture(success([second: 2]))))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()

        expect:
        queue.initialPending*.label == ['first', 'second']

        when:
        queue.publisher.subscribe(subscriber)
        def results = subscriber.awaitResults()

        then:
        results.collectMany { it.incremental ?: [] }*.data == [[first: 1], [second: 2]]
        results.last().hasNext == false
    }

    def 'failure without counted errors remains a failure'() {
        given:
        def root = new DeliveryGroup(null, path, null)
        def work = new IncrementalWork()
        work.addGroup(root)
        work.addTask(task([root], CompletableFuture.completedFuture(new WorkResult(null, [], new IncrementalWork(), true))))
        def queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()

        when:
        queue.publisher.subscribe(subscriber)

        then:
        subscriber.awaitResults() == [[completed: [[id: '0']], hasNext: false]]
    }
    def 'aborting a fulfilled computation preserves its cached result without reevaluation'() {
        given:
        def starts = new AtomicInteger()
        def work = new IncrementalWork()
        def outcome = CompletableFuture.completedFuture(success([a: 'A'], work))
        def computation = new DeferredTask([], path, work, { starts.incrementAndGet(); outcome })

        when:
        def first = computation.result()
        computation.abort()

        then:
        computation.result().is(first)
        starts.get() == 1
    }

}
