package graphql.execution.incremental.v2

import graphql.ExecutionInput
import graphql.ExperimentalApi
import graphql.execution.preparsed.PreparsedDocumentEntry
import graphql.execution.preparsed.PreparsedDocumentProvider

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

class IncrementalPreparsedCache implements PreparsedDocumentProvider {
    final Map<List, PreparsedDocumentEntry> entries = new ConcurrentHashMap<>()
    final AtomicInteger computations = new AtomicInteger()
    boolean partitionValidation = true

    CompletableFuture<PreparsedDocumentEntry> getDocumentAsync(ExecutionInput input, Function<ExecutionInput, PreparsedDocumentEntry> compute) {
        def key = [input.query, partitionValidation && input.graphQLContext.getBoolean(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2)]
        def entry = entries.computeIfAbsent(key, {
            computations.incrementAndGet()
            compute.apply(input)
        })
        CompletableFuture.completedFuture(entry)
    }
}
