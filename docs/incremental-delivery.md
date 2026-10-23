# Incremental delivery draft

The new implementation is opt-in and coexists with the existing defer implementation.
It targets [graphql/graphql-spec PR #1110 at 045e193](https://github.com/graphql/graphql-spec/tree/045e19363c2b55f127960bd3b5e8072a15b29aec), matching the supplied
`graphql-lean` model at `aa3f4ee870c7b865b81720aa672d9052a27b3695`.

## Enable the new implementation

Add the directives to the schema and enable the new execution-context option:

```java
GraphQLSchema incrementalSchema = GraphQLSchema.newSchema(schema)
        .additionalDirective(Directives.DeferDirective)
        .additionalDirective(Directives.StreamDirective)
        .build();
GraphQL graphQL = GraphQL.newGraphQL(incrementalSchema).build();
ExecutionInput input = ExecutionInput.newExecutionInput()
        .query("{ users @stream(initialCount: 1) { id ... @defer { name } } }")
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
open delivery IDs, and `getIncrementalItemPublisher()` returns a single-subscriber
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
ID and fatal errors, when present. `IncrementalPayloadV2` carries either deferred object
`data` or streamed `items`, an ID, scoped errors, and an optional `subPath` relative to
its pending notice. Nullable item values are retained. New payloads omit the legacy
`path` and `label` fields; clients associate them through the pending ID.

`getPending()` and `getCompleted()` return empty lists for legacy results. Legacy
`DeferPayload` and `StreamPayload` serialization remains unchanged. Consumers opting
into the new implementation should handle `IncrementalPayloadV2` and the new notice
lists. All new public payloads snapshot their collections and have builders and
`transform()` methods.

## Execution and lifecycle

The implementation follows the alternative work-queue model from the Lean porting
guide. Java object identities distinguish usages, groups, tasks and sources. Immutable
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
Child notices and held streams are released when their producer's data is
published. Failed subtrees are removed iteratively, and last-owner removal disposes
unpublished work during that walk. Failed ancestry cannot revive through late results.

`@stream` affects the outermost list. It completes the initial prefix normally and
completes subsequent items sequentially. A prefix ending exactly at exhaustion retains
an empty stream boundary; a prefix extending beyond exhaustion returns an ordinary list. Source pulls run independently of wire delivery. Sources use the engine's existing iterable and
array representations; streamed items may also be `CompletableFuture` values. One
original item may await graph admission per source. Admission acknowledges that item
before advancing its source. Subscriber demand controls graph reduction, bounding
source progress even with a paused consumer. Execution yields after 64 serialized
commands to avoid monopolizing the subscribing thread with a long synchronous source.

Cancellation stops activation immediately, disposes in-flight producer scopes, closes an
`AutoCloseable` iterator (or its iterable when the iterator is not closeable), and
suppresses late mapping. Source exhaustion and cancellation close the selected resource
at most once. Queue records and wire IDs are retired at response completion, scoped
DataLoader accounting is released, and the response-completion instrumentation hook is
invoked once after cancellation cleanup requests. `ExecutionInput.cancel()` also closes
the incremental publisher. Delivered patches retain all collected diagnostics. A discarded deferred object or streamed item reports
its selected escaping cause in the completion notice, separately from the complete
diagnostic inventory. Ordinary results and the initial envelope retain collected errors.

Mutation root fields retain serial execution. Active incremental directives are
forbidden in subscriptions, including conditional directives that become enabled by
coerced variables. Directive validation checks placement, static unique labels, list
fields, and overlapping streamed selections. Request validation receives the request's
GraphQL context. If a custom preparsed-document cache is shared across different
validation configurations, its keys must distinguish those configurations.

## Integration boundary and review order

The v2 collector, planner, work queue and publisher live in
`graphql.execution.incremental.v2`. `IncrementalExecution` owns operation setup and
initial response assembly. `IncrementalFieldCompletion` and `IncrementalObjectCompletion`
reuse ordinary field execution through callbacks from the existing strategy. Legacy
result implementations are unchanged; v2 results implement the shared interfaces.

Review payloads first, then collection and planning, queue and publication, and finally
the guarded engine hooks and compatibility tests. V2 is disabled by default. Stream support extends the same work queue with scoped sources. The stream source lifecycle
is reviewed after the defer engine and oracle foundations.

## Verification

Spock tests cover the wire format, legacy compatibility, shared owners, empty/nested
fragments, null propagation, stream prefixes, nested lists, nullable and fatal item
errors, DataLoader integration, demand, cancellation, late futures and resource cleanup.
Transition regressions reuse warm preparsed documents across flag combinations and separate
validation cache entries by configuration. Native tests cover inherited interface and union
conditions, serial mutation prefixes with lazy nested work, held-source cancellation, and
100 barrier-released cancellation/completion races.
The optional [Java–Lean differential harness](../fuzz/incremental/README.md) independently
executes valid generated operations and replays the shared JavaScript semantic corpus.
The small harness covers sync and completed-future resolvers, inline and named documents,
rotating demand, replay and shrinking, including the added stream semantic corpus.
Delayed schedules, native fuzz properties and coverage profiles are a separate follow-up.

Differential agreement tests the finite modeled subset. It does not constitute a formal
Java refinement proof or certify arbitrary asynchronous callback schedules, infinite sources,
cleanup exceptions, or transport behavior. The engine does not implement an HTTP
transport; applications serialize each result's `toSpecification()` map.
