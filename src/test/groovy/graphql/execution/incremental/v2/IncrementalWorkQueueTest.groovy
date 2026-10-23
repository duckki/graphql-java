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
        results[1].completed*.id == ['0']
        results[1].hasNext
        results[2].completed*.id == ['2']
        results[2].completed[0].errors*.message == ['shared failure']
        !results[2].hasNext
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
        def source = new CloseableItems(values: [1])
        held.addStream(new ItemStream(path, null, source.iterator(), 0, source, held, { raw, index, itemWork -> CompletableFuture.completedFuture(success([value: raw])) }))
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
        source.closed.get() == 0

        when:
        parentResult.complete(success([name: 'parent']))
        subscriber.awaitResults()
        held.checkRunning()

        then:
        thrown(java.util.concurrent.CancellationException)
        source.closed.get() == 1

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

    def 'successful outcome is adopted while a rejected dormant co-owner remains live'() {
        given:
        def parent = new DeliveryGroup(null, path, 'parent')
        def child = new DeliveryGroup(parent, path, 'child')
        def other = new DeliveryGroup(null, path, 'other')
        def failureOwner = new DeliveryGroup(null, path, 'failure')
        def parentResult = new CompletableFuture<WorkResult>()
        def producerResult = new CompletableFuture<WorkResult>()
        def otherFailure = new CompletableFuture<WorkResult>()
        def childFailure = new CompletableFuture<WorkResult>()
        def held = new IncrementalWork()
        def source = new CloseableItems(values: [1])
        held.addStream(new ItemStream(path, null, source.iterator(), 0, source, held,
                { raw, index, itemWork -> CompletableFuture.completedFuture(success([value: raw])) }))
        def work = new IncrementalWork()
        [parent, child, other, failureOwner].each { work.addGroup(it) }
        work.addTask(task([parent], parentResult))
        work.addTask(task([child, other], producerResult, held))
        work.addTask(task([other], otherFailure))
        work.addTask(task([child, failureOwner], childFailure))
        def subscriber = new IncrementalTestSubscriber()
        new IncrementalWorkQueue(work, {}).publisher.subscribe(subscriber)

        when:
        childFailure.complete(failure('child failed'))
        otherFailure.complete(failure('other failed'))
        producerResult.complete(success([held: true], held))

        then:
        source.closed.get() == 0
        held.streams.empty
        subscriber.results.collectMany { it.incremental ?: [] }.empty

        when:
        parentResult.complete(success([parent: true]))
        def updates = subscriber.awaitResults()

        then:
        source.closed.get() == 1
        updates.collectMany { it.incremental ?: [] }.every { !it.data.containsKey('held') }
        updates.collectMany { it.completed ?: [] }*.id == ['2', '1', '0', '3']
    }

    def 'cancellation follows task-before-child depth-first order despite declaration order'() {
        given:
        def root = new DeliveryGroup(null, path, 'root')
        def left = new DeliveryGroup(root, path, 'left')
        def leaf = new DeliveryGroup(left, path, 'leaf')
        def right = new DeliveryGroup(root, path, 'right')
        def calls = []
        def tasks = [root, left, leaf, right].withIndex().collect { owner, index ->
            def held = new IncrementalWork()
            def resource = { calls.add(index) } as AutoCloseable
            held.addStream(new ItemStream(path, null, [].iterator(), 0, resource, held,
                    { raw, itemIndex, itemWork -> CompletableFuture.completedFuture(success([unused: true])) }))
            def computation = task([owner], new CompletableFuture<WorkResult>(), held)
            computation.result()
            computation
        }
        def work = new IncrementalWork()
        [root, left, right, leaf].each { work.addGroup(it) }
        tasks.each { work.addTask(it) }
        def queue = new IncrementalWorkQueue(work, {})

        when:
        queue.cancel()

        then:
        calls == [0, 1, 2, 3]
    }

    def 'source failure preserves previously admitted child work'() {
        given:
        def child = new DeliveryGroup(null, path, 'child')
        def result = new CompletableFuture<WorkResult>()
        def source = new CloseableItems(values: [1, 2])
        def work = new IncrementalWork()
        def stream = new ItemStream(path.segment('values'), null, source.iterator(), 0, source, work,
                { raw, index, itemWork ->
                    if (index == 1) return CompletableFuture.completedFuture(failure('source failed'))
                    itemWork.addGroup(child)
                    itemWork.addTask(task([child], result))
                    CompletableFuture.completedFuture(new WorkResult(raw, [], itemWork, false))
                })
        work.addStream(stream)
        def subscriber = new IncrementalTestSubscriber()
        new IncrementalWorkQueue(work, {}).publisher.subscribe(subscriber)
        new spock.util.concurrent.PollingConditions(timeout: 5).eventually {
            assert subscriber.results.collectMany { it.completed ?: [] }*.id == ['0']
        }

        when:
        result.complete(success([child: true]))
        def updates = subscriber.awaitResults()

        then:
        updates.collectMany { it.incremental ?: [] }.find { it.data == [child: true] }
        updates.collectMany { it.completed ?: [] }*.id == ['0', '1']
        source.closed.get() == 1
        !updates.last().hasNext
    }

    def 'reentrant cancellation cannot erase later held-source cleanup targets'() {
        given:
        def owner = new DeliveryGroup(null, path, 'owner')
        def failure = new CompletableFuture<WorkResult>()
        def closed = [0, 0]
        IncrementalWorkQueue queue
        def work = new IncrementalWork()
        work.addGroup(owner)
        (0..1).each { index ->
            def held = new IncrementalWork()
            def resource = {
                closed[index]++
                if (index == 0) queue.cancel()
            } as AutoCloseable
            held.addStream(new ItemStream(path, null, [].iterator(), 0, resource, held,
                    { raw, itemIndex, itemWork -> CompletableFuture.completedFuture(success([unused: true])) }))
            work.addTask(task([owner], CompletableFuture.completedFuture(success([held: index], held)), held))
        }
        work.addTask(task([owner], failure))
        queue = new IncrementalWorkQueue(work, {})
        def subscriber = new IncrementalTestSubscriber()
        queue.publisher.subscribe(subscriber)

        when:
        failure.complete(this.failure('failed'))

        then:
        queue.stopped
        closed == [1, 1]
        subscriber.results.empty
    }

}
