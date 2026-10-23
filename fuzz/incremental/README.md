# Java–Lean incremental delivery fuzzing

This target executes graphql-java independently against the supplied Lean executor and
alternative queue. The original generator, oracle protocol and 31 semantic fixtures were
adapted from `graphql-incremental-delivery-js/fuzz`; a further fixture preserves explicit
null labels, four more cover interfaces and unions, and two combine initial diagnostics
with failed delivery causes. Java does not call the JavaScript executor. The enhanced
generator and native property lanes address the gaps measured in the [coverage evaluation](evaluation.md).

## Profiles and commands

Use a clean graphql-lean checkout at `aa3f4ee870c7b865b81720aa672d9052a27b3695`, with its
imported libraries built and Lean 4.35.0 (`lake`) on PATH. The normal Java build has no Lean dependency.
There are no new dependencies; the optional target uses existing test-scoped Jackson and
the project's existing JaCoCo plugin.

```sh
./gradlew incrementalDifferentialFuzz \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=smoke
```

| Profile | Generated inputs | Structural depth / selection budget / list length | Native cancellation inputs |
|---|---:|---|---:|
| `legacy` | 1,000 | 3 / 36 / 3, original byte generator | 0 |
| `smoke` | 100 | 4 / 64 / 6 | 100 |
| `ci` (default) | 2,000 | 4 / 64 / 6 | 100 |
| `nightly` | 50,000 | 6 / 96 / 8 | 1,000 |

The enhanced profiles test four resolver representations and both inline and named
fragment documents: eight comparisons per input. Subscriber demand rotates deterministically
among one update, bursts of three, and unbounded demand. `legacy` keeps the original
synchronous/completed-future, inline-document, one-update-demand matrix for old seed replay.
The 38 checked-in corpus cases run before generated inputs.

Useful overrides are `fuzzCases`, unsigned 32-bit `fuzzSeed`, `fuzzHostCases`,
`fuzzResolvers=sync,promise,items,async`, `fuzzDocuments=inline,named`, and
`fuzzDemands=one,burst,all`. Selecting just one demand profile applies it to every comparison.
Seeds wrap at the unsigned 32-bit boundary. `GRAPHQL_INCREMENTAL_LEAN_PROJECT` can replace
`leanProject`; `leanOracle` can select a protocol-compatible executable.

A bounded longer campaign can use both a case limit and an elapsed-time budget:

```sh
./gradlew incrementalDifferentialFuzz \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=nightly \
  -PfuzzDurationSeconds=1800 -PfuzzOutput=build/incremental-fuzz/nightly
```

The time budget stops between generated inputs; it does not interrupt an in-flight comparison
or waive coverage requirements. Reports distinguish requested and completed input counts.
These are profiles to run explicitly; this change creates no scheduled jobs or remote CI jobs.

## Generation and comparison

The new generator gives 40% of seeds to structural generation and distributes the remaining
60% across eleven targeted families: shared owners, deep defer, prefix boundaries, nested lists,
streamed children, fatal tails, deferred cuts, conditional directives, shared composites,
abstract-type defer, and abstract-type streams.
Shared-failure cases also create held streams. Labels may be omitted, null, or unique strings.
Lists include empty, nullable, nested, and non-null item/container combinations. Prefix counts
cover zero, one, list length, beyond length, and variables. Targeted defer nesting exceeds the
structural-depth limit; reports record the actual maximum selection depth.

A seeded PRNG supplies fresh choices throughout generation. Data is generated from the fields
actually selected, merging repeated source fields, with a shared 192-list-item budget to bound
recursive expansion. This avoids the original 256-byte cursor's zero-filled tail and spending
entropy on unselected recursive objects. The fragment-free IR, source data and variables are
saved in every replay artifact.

The fixture includes two concrete implementations, an interface extending another interface,
and a union. Structural generation can select interface and union fields; data generation
chooses a compatible runtime object and filters inapplicable type conditions. Named-fragment
lowering derives each child type from the schema. The Lean adapter serializes object
implementations, interface inheritance and union members into the pinned model's existing
schema representation; its execution and queue algorithms remain unchanged.

`sync` returns direct values. `promise` returns completed or rejected field futures. `items`
additionally wraps active streamed items in futures. `async` settles fields and streamed items
on two scheduler threads after seed/path-derived delays of 1–3 ms. Async completion order can
vary with host scheduling; a saved seed reproduces the input and delay policy, not an exact
thread interleaving.

The oracle is one persistent process per campaign, pinned to the model commit and tracked-file
cleanliness. It imports the actual Lean executor and queue; the adapter decodes inputs and
selects an admissible finite task-first schedule through existing model transitions. The
execution policy is explicitly eager. The adapter projects `TaskOutcome.deliveryResult`,
retains ordered declarations, and dispatches queued group-failure notices through the
model transitions. Its default startup observation is asynchronous; native synchronous
cache visibility and FIFO interleavings are covered by focused Spock tests. One modeled result is cached for the consecutive native
variants of an identical IR; schemas are reused per resolver mode.

Every common-domain query must validate. Comparisons require exact initial data and initial
error counts, then exact reconstructed final data and total error counts. No invalid query or
model disagreement is silently excluded. Native messages/locations are outside Lean's counted
error abstraction. The independent observer checks pending IDs, patch paths, duplicate or
unknown IDs/completions, final `hasNext`, and remaining open IDs. Chunk boundaries, announcement
multiplicity and payload order are not compared as timing invariants.

Structural cases in the generated differential domain inject resolver errors only into
nullable fields and keep non-null values valid. Targeted fatal families retain single-origin
failures, shared owners and child-work cuts. This restriction matters: multiple fatal siblings
or competing failed tasks can legitimately produce different counts under eager Lean and
native cancellation schedules. Merely ignoring counts or accepting a numeric range would
hide lost errors. The same structural seeds are also generated with unrestricted failures
for the separate native lane below; they are not passed off as exact Lean comparisons.
The legacy generator and explicit common corpus retain their original data and strict checks.

The named-document renderer converts the IR into a finite, reused fragment DAG while retaining
directives on spreads. Its native result is compared with the inline Lean result as an
inlining metamorphic check. Lean itself still models the fragment-free IR; arbitrary named
fragment graphs and their validation are not formally modeled by this target.

## Native property lanes

For structural seeds among the first `fuzzHostCases` inputs, a second data variant permits
multiple non-null/resolver failures. Every selected resolver/document pair checks its full
incremental lifecycle, verifies every emitted error against a selected primitive origin,
and rejects duplicate paths within a delivery ID (shared owners may each report an origin).
Removing incremental directives then requires exact data, error counts and error-path
multisets between the new and existing ordinary executors. A checked-in native corpus and
Spock regression preserve the causal-order case and the earlier-nullable/later-fatal error
pair. Failed completions contain the selected escaping cause; delivered caught-null
patches and ordinary responses retain all collected diagnostics. These checks do not
assert that all eagerly modeled errors must execute, or prove that every asynchronous recording history is preserved. Reports count `faultChecks`
separately; they are native properties, not Lean comparisons.

The first `fuzzHostCases` generated inputs also run with asynchronous resolvers, named
fragments and tracked stream sources. Six policies cover cancellation before activation,
after one or two updates, invalid demand, execution-input cancellation, and rejecting a
second subscriber. The response-prefix observer checks delivered IDs and paths. Every acquired
source must close exactly once, through either its iterator or iterable, and pulls must remain
within the initial prefix plus published items plus one prefetch slot. Twelve milliseconds
allow the harness's bounded late futures to settle before post-cancellation checks; this is
not a quiescence guarantee for arbitrary application futures.

Four additional native cases reject item futures in nullable and non-null streams, in both
the prefix and tail, and require the expected data, one originating error, and source closure.
These deliberately failing item representations stay outside the shared scalar-value domain.

128 seeded validation cases vary subscription directive conditions and repeated named spreads.
Each checks an allowed/rejected subscription outcome after coercion and an invalid query whose
diagnostics must be stable across two executions. Negative families include placement, counts,
labels, overlapping streams, mutation roots, missing fields and malformed syntax. These native
properties are reported separately from Lean comparison counts. `fuzzHostCases=0` disables all
native property lanes. The normal Spock harness tests exercise these lanes without Lean.

## Coverage, witnesses and replay

Enhanced profiles require semantic bins for ordinary/incremental results, shared and child
notices, defer/stream patches, scoped/fatal errors, null items, all label forms, deep selections
and nested lists. Additional bins require interface/union syntax and observed patches for
both concrete implementations, an empty stream completion, and initial diagnostics
coexisting with a fatal completion. Sufficient cancellation campaigns also require all six host policies and
source closure. `fuzzRequireBins=false` disables these gates for a deliberately narrow run.
Corpus-only and replay runs disable gates by default.

Reports under `build/incremental-fuzz/report.json` include the profile/matrix, source and oracle
hashes, seed, elapsed time, throughput, unique IR/operation counts, semantic bins and first
witnesses. Semantic coverage counts each reached bin once per comparison. `fuzzRetainCorpus=true`
saves the first novel semantic witnesses under the output's `witnesses/` directory; they can
be replayed as a corpus. Host closure bins count acquired sources. The legacy profile also
reports exhausted-buffer inputs and zero-fallback reads.

To measure actual JVM branches separately from semantic bins:

```sh
./gradlew incrementalDifferentialFuzz incrementalFuzzCoverageReport \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=ci -PfuzzCoverage=true
```

The JaCoCo report is under `build/reports/jacoco/incrementalFuzzCoverageReport/` in XML and HTML.
It measures the new execution package and incremental payload classes. The latter also include
legacy payloads, so their aggregate is not the new executor's coverage. Each instrumented run
replaces the execution data; copy its report before measuring another profile. Use the same
production class files when comparing profiles.

A semantic mismatch saves original and minimized IR, rendered query, resolver/document/demand
policy, both original and minimized model/native observations and errors. The shrinker removes selections while keeping
composite selections nonempty; only reproduced semantic failures of the original diagnostic
category are accepted, up to 100
attempts. Infrastructure/protocol failures are retained without semantic shrinking. Native
cancellation and fault-property failures use the same shrinker; fixed item/validation failures record their
explicit replay lane and parameters.

```sh
./gradlew incrementalDifferentialFuzz \
  -PleanProject=/path/to/graphql-lean \
  -PfuzzReplay=build/incremental-fuzz/failures/123-async-named-burst.json
```

Replay restores the saved lane and native policies unless explicitly overridden. `fuzzReplay`
also accepts common JS native-Lean artifacts containing an `input` object. `fuzzCases=0` runs
only a selected `fuzzCorpus` directory. Fixed bugs should become checked-in corpus cases and
focused Spock regressions. Three corruption sentinels check data, error-count, and ID-lifecycle
sensitivity; `fuzzSentinel=false` disables them.

This is deterministic randomized and metamorphic testing with offline JVM coverage measurement
and semantic-witness retention. It is not online coverage-guided Jazzer/libFuzzer fuzzing or an
exact queue-state/effect refinement proof. Arbitrary schemas, abstract-type layouts beyond
the fixed fixture, arbitrary parser
bytes, mutations, transport, unbounded sources, cleanup exceptions and general callback
schedules remain outside the differential claim.

## Defer-only profile

`-PfuzzProfile=defer` retains the small PR 2 generator and defaults to synchronous and
completed-future resolvers, inline/named fragments and rotating demand. Its default
corpus selection excludes stream cases and its host/coverage gates are disabled.
Explicit `fuzzCorpus` or `fuzzReplay` still runs the requested cases. `unbounded` remains
an accepted alias for the `all` demand policy.

Historical measurements in `evidence/` refer to the source hashes recorded in those
files. The split introduces separate v2 result implementations and smaller engine hooks;
its coverage denominator and source hash must be measured anew.
