# Incremental delivery draft

The new implementation is opt-in and coexists with the existing defer implementation.
It targets [graphql/graphql-spec PR #1110 at 045e193](https://github.com/graphql/graphql-spec/tree/045e19363c2b55f127960bd3b5e8072a15b29aec), matching the supplied
`graphql-lean` model at `aa3f4ee870c7b865b81720aa672d9052a27b3695`.

## Enable the new implementation

Add the directives to the schema and enable the new execution-context option:

```java
GraphQLSchema incrementalSchema = GraphQLSchema.newSchema(schema)
        .additionalDirective(Directives.DeferDirective)
        .build();
GraphQL graphQL = GraphQL.newGraphQL(incrementalSchema).build();
ExecutionInput input = ExecutionInput.newExecutionInput()
        .query("{ users { id ... @defer { name } } }")
        .graphQLContext(Map.of(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2, true))
        .build();
ExecutionResult initial = graphQL.execute(input);
```

`ENABLE_INCREMENTAL_SUPPORT` continues to select the legacy response format.
`ENABLE_INCREMENTAL_SUPPORT_V2` enables the new implementation independently and takes
precedence when both options are true. Disabling the new option restores the legacy
implementation. The existing eager-defer option applies to the legacy implementation;
new incremental work starts when its result publisher is subscribed.

The result remains an `IncrementalExecutionResult`. Its `getPending()` method describes
open defer delivery IDs, and `getIncrementalItemPublisher()` returns a single-subscriber
Reactive Streams publisher of `DelayedIncrementalPartialResult` updates. Ordinary
operations and operations with no effective incremental work return an ordinary
`ExecutionResult`, with no `hasNext` entry.

## Response format

For `{ user { id ... @defer(label: "details") { name } } }`, the initial result can be:

```json
{
  "data": { "user": { "id": "1" } },
  "pending": [{ "id": "0", "path": ["user"], "label": "details" }],
  "hasNext": true
}
```

A subsequent result completes the ID:

```json
{
  "incremental": [{ "id": "0", "data": { "name": "Ada" } }],
  "completed": [{ "id": "0" }],
  "hasNext": false
}
```

`PendingPayload` carries an ID, path and optional label, preserving an explicitly supplied null label. `CompletedPayload` carries an
ID and fatal errors, when present. `IncrementalPayloadV2` carries deferred object `data`, an ID, scoped errors, and an optional `subPath` relative to
its pending notice. New payloads omit the legacy
`path` and `label` fields; clients associate them through the pending ID.

`getPending()` and `getCompleted()` return empty lists for legacy results. Legacy
`DeferPayload` and `StreamPayload` serialization remains unchanged. Consumers opting
into the new implementation should handle `IncrementalPayloadV2` and the new notice
lists. All new public payloads snapshot their collections and have builders and
`transform()` methods.

## Execution and lifecycle

The implementation follows the alternative work-queue model from the Lean porting
guide. Java object identities distinguish usages, groups, tasks and scopes. Immutable
defer-context overlays share inherited bindings. Ordered declarations remain separate from task contributors and follow immediate-field
work; an absent parent cannot promote a late child to a root. An iterative ancestry index partitions
merged fields into tasks, preserving collector and owner order. Overlapping selections
resolve a field once; the queue publishes a shared task's value once and chooses an
announced owner with the deepest path.

Each exposed scope is registered completely before ownership transfers or child
computations start. Pending counters track successful settlement. Rejected outcomes stay on their computations,
including failures with zero counted errors, until their remaining owners are removed. Empty defer groups are
pruned. Startup inspects every already available task rejection before starting private
work. A cached failure removes its group immediately and queues one aggregated completion
notice at its FIFO position. Queued failure notices keep the queue alive after roots disappear.
Child notices are released when their producer's data is
published. Failed subtrees are removed iteratively, and last-owner removal disposes
unpublished work during that walk. Failed ancestry cannot revive through late results.

Publisher demand controls delivery. Each deferred computation starts lazily after
subscription. Cancellation stops activation, disposes in-flight producer scopes and
suppresses late results. Failed ancestry cannot be revived by a late future. Queue
records and wire IDs are retired at response completion, scoped DataLoader accounting
is released, and response-completion instrumentation runs once. `ExecutionInput.cancel()`
also closes the publisher. Delivered patches retain all collected diagnostics. A discarded deferred object reports
its selected escaping cause in the completion notice, separately from the complete
diagnostic inventory. Ordinary results and the initial envelope retain collected errors.

Mutation root fields retain serial execution. Active incremental directives are
forbidden in subscriptions, including conditional directives that become enabled by
coerced variables. Directive validation checks placement and static unique labels. Request validation receives the request's
GraphQL context. If a custom preparsed-document cache is shared across different
validation configurations, its keys must distinguish those configurations.

## Integration boundary and review order

The v2 collector, planner, work queue and publisher live in
`graphql.execution.incremental.v2`. `IncrementalExecution` owns operation setup and
initial response assembly. `IncrementalFieldCompletion` and `IncrementalObjectCompletion`
reuse ordinary field execution through callbacks from the existing strategy. Legacy
result implementations are unchanged; v2 results implement the shared interfaces.

Review payloads first, then collection and planning, queue and publication, and finally
the guarded engine hooks and compatibility tests. V2 is disabled by default. This first
stage supports defer only; stream execution and oracle tooling are separate follow-ups.

## Verification

Spock tests cover wire serialization and snapshots, legacy compatibility, shared owners,
empty and nested fragments, list indices, null propagation, asynchronous errors,
DataLoader integration, cancellation and late futures. Transition tests reuse warm
preparsed documents across flag combinations and separate validation cache entries by
configuration. Native tests cover inherited interfaces, unions and serial mutations with
lazy nested work. The alternative model is retained from the supplied Lean porting guide.

The optional [defer differential harness](../fuzz/incremental/README.md) adds exact
comparisons with the pinned Lean oracle, saved corpus replay and shrinking. It is
separate from ordinary tests and adds no runtime dependencies.
