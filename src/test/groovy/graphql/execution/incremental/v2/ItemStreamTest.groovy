package graphql.execution.incremental.v2

import graphql.execution.ResultPath
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class ItemStreamTest extends Specification {
    def 'source cancellation disposes an unresolved item scope while other request work remains live'() {
        given:
        def raw = new CompletableFuture()
        def starts = new AtomicInteger()
        def source = new CloseableItems(values: [raw])
        def requestWork = new IncrementalWork()
        def stream = new ItemStream(ResultPath.rootPath().segment('values'), null, source.iterator(), 0, source, requestWork,
                { value, index, work -> value.thenApply {
                    work.checkRunning()
                    starts.incrementAndGet()
                    new WorkResult(it, [], work, false)
                } })
        def result = stream.next()

        when:
        stream.cancel()
        raw.complete(1)

        then:
        starts.get() == 0
        result.completedExceptionally
        source.closed.get() == 1
        requestWork.checkRunning() == null
    }
}
