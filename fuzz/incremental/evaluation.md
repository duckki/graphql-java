# Incremental fuzzing evaluation

## Updated Lean model (2026-10-22)

The current port uses Lean `aa3f4ee8` and the JS port at `35d39bff` as a test
reference. It separates collected diagnostics from escaping delivery causes, retains
rejections for dormant owners, checks all available rejections before private task startup,
and forwards cached failure notices at their FIFO position. Ordered declarations,
last-owner cleanup, deep iterative cancellation and empty stream boundaries have focused
regressions adapted from the JS tests.

The new CI campaign passed **16,304 exact comparisons** from 2,000 generated inputs and
38 corpus cases, plus 100 cancellation checks, 352 native fault checks, 128 validation
pairs and four rejected-item checks. All corruption sentinels and semantic gates passed.
The two new gates observed 16 initial-error/fatal-completion combinations and 1,120 empty
stream completions. A further sample of 250 inputs at nightly bounds crossed unsigned
seed wraparound and passed **2,304 exact comparisons**; it is not a full nightly campaign.

Fuzz-only coverage of the updated execution package is **441/534 branches (82.6%)**
and **993/1,136 lines (87.4%)**. Production code and its coverage denominator changed,
so these measurements cannot isolate the effect of additional fuzz inputs. The focused
suite passed all 129 regressions on Java 11, 17, 21 and 25. The full Java 25 suite
ran 5,901 tests with no failures and eight expected skips. TestNG passed 141 tests with
the exact 49 expected skips; assembly, shaded-consumer, annotation and Javadoc checks
passed. Source hashes, commands and runtime checks are recorded in [model-update evidence](evidence/model-update-2026-10-22.json).

The historical measurements below use the previous model and source revisions.

The campaigns below describe the original implementation before the isolation refactor.
They retain their original source hashes and coverage denominators. The local review
stack uses separate v2 result implementations and extracted integration adapters;
these historical percentages do not measure that refactored source. A fresh bounded
campaign for the review stack is recorded separately below.

The original harness provided useful exact Lean comparisons but left much of the new
execution package unvisited. Its 10,000 generated inputs plus 32 corpus cases produced
20,064 comparisons and covered **314 of 464 branches (67.7%)** and **784 of 994 lines
(78.9%)**. A separate entropy audit found that **3,532 of 10,000 inputs (35.32%)**
exhausted the fixed 256-byte generator buffer, followed by 40,472 zero-fallback reads.
More seeds alone were a poor way to explore deeper execution shapes.

The enhanced profiles replace that buffer with a seeded PRNG, generate data from selected
fields, and devote 40% of inputs to structural generation and 60% to nine targeted families.
They cover longer and nested lists, prefix boundaries, shared owners, deep defer, child
streams, fatal tails and parent cuts. Four resolver modes, two document forms and three
demand policies exercise completion and delivery behavior. Native lanes add cancellation,
resource closure, rejected item futures, validation, multi-failure error origins and ordinary
execution parity. See [commands and comparison boundaries](README.md).

The final CI measurement covers **378 of 464 branches (81.5%)** and **864 of 994 lines
(86.9%)**, a gain of 64 branches and 80 lines without changing production code. It runs
20,256 exact comparisons from 2,500 generated inputs, plus the separately counted native
properties. Coverage percentages describe this measured workload, not every profile or
arbitrary callback schedule.

## Error-domain finding

The first wider run exposed an assumption in the comparator, rather than duplicate native
errors. Seed 379 at the nightly bounds reconstructed identical final data but delivered
25 native errors versus 10 under Lean's task-first schedule. Lean's work tree contained
the larger failed task, but an earlier failed child retired its owner before that task could
publish. Java processed the owning task first. Shrinking also reached a case with one native
error versus two eager errors because a fatal ordinary list completion skipped sibling work.
These are permitted differences described by the supplied model's porting guide.

Exact counted-error comparisons now have an explicit generated domain: structural resolver
errors affect nullable fields, while targeted fatal cases retain single-origin failures.
The original common corpus remains strict. The same structural seeds also generate
unrestricted failures for native properties; they are not counted as Lean agreement.
Those properties validate emitted primitive origins and uniqueness per delivery ID, and
require the new and existing ordinary executors to agree after removing incremental
directives. The [causal-order fixture](native-corpus/causal-error-order.json) and Spock
regressions check preservation of an earlier nullable error followed by a non-null failure.
No error counts are clamped or ignored in an exact comparison.

Failure artifacts now retain both original and minimized observations. The shrinker accepts
only a reproduced diagnostic category, so a smaller different failure cannot erase the
original evidence. Corpus replay restores each artifact's native lane and policies.

## Measured campaigns

Coverage uses only `incremental-fuzz.exec`, independently of Spock's execution data. All
measurements use the same production source and class files, Java 25, and Lean commit
`0c0cd04cdf4d05805a9a0510fe341d10889ffa9f`. Legacy payload classes are excluded from the
execution-package percentages. Counts include the 32 common corpus cases.

| Campaign | Generated inputs | Exact Lean comparisons | Cancellation checks | Native fault checks |
|---|---:|---:|---:|---:|
| Original baseline | 10,000 | 20,064 | 0 | 0 |
| Final CI, seed 1 | 2,500 | 20,256 | 250 | 816 |
| Wider nightly sample, seed 4294967000 | 1,000 | 8,256 | 1,000 | 3,192 |

The wider sample passed all semantic gates and three corruption sentinels, crossed the
unsigned 32-bit seed boundary, and reached selection depth 10. It also passed 128 paired
validation checks and four rejected-item checks. This is a bounded sample of the nightly
profile, not a completed 50,000-input campaign. The CI campaign also passed all gates,
sentinels, 128 validation pairs and four rejected-item checks. Its maximum selection depth
was 8, with 2,477 unique IR inputs and 1,617 unique operations. The full Java 25 suite
reported 5,832 passed tests and eight skipped tests; the final focused run passed 69 tests.
Pins, source hashes and measurements are saved in
[the machine-readable evidence](evidence/evaluation-2026-10-14.json).

```sh
./gradlew incrementalDifferentialFuzz incrementalFuzzCoverageReport \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=legacy \
  -PfuzzCases=10000 -PfuzzCoverage=true -PfuzzOutput=build/incremental-fuzz/baseline

./gradlew incrementalDifferentialFuzz incrementalFuzzCoverageReport \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=ci \
  -PfuzzCases=2500 -PfuzzHostCases=250 -PfuzzCoverage=true \
  -PfuzzOutput=build/incremental-fuzz/final-ci

./gradlew incrementalDifferentialFuzz \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=nightly \
  -PfuzzCases=1000 -PfuzzSeed=4294967000 \
  -PfuzzOutput=build/incremental-fuzz/nightly-sample
```

The original entropy audit took 20.85 seconds without JVM coverage instrumentation. The
wider sample took 86.78 seconds including native lanes. Final CI took 83.97 seconds with
JVM coverage and native lanes. These are harness wall times on the
development machine, excluding Gradle compilation and Spock; the different workloads make
them unsuitable as executor performance benchmarks.

## Remaining coverage

This is deterministic randomized testing with offline JaCoCo measurement and semantic
witness retention. It does not feed JVM edges back into generation, and it does not compare
every native queue state/effect against Lean. Semantic gates distinguish selected syntax
from observed wire outcomes; neither is a proof of all scheduler interleavings.

The fixed schema limits schema-variation coverage. The expanded fixture now includes two
concrete implementations, inherited interfaces and a union; other abstract-type layouts
remain outside the measured domain. Arbitrary
named fragment graphs, parser bytes, differential mutation semantics, infinite sources, cleanup failures
and general application callback schedules remain outside the differential claim. Unused
internal builder methods also lower line coverage. Broader native scheduling and a captured
queue-event replay lane would be the next improvements to support exact error comparisons
on unrestricted asynchronous inputs.

## Pre-push coverage expansion

The follow-up adds four common corpus cases and two targeted families for abstract defer
and abstract streams. Structural generation also selects interface and union fields. Runtime
data is generated from a compatible concrete type, filtering nonmatching fragment conditions,
and the named renderer derives fragment types from the schema. The adapter lowers interface
inheritance and union members into the existing Lean schema definitions without changing
the model's execution algorithms. New gates require observed patches from both concrete
implementations, separately from interface/union syntax coverage.

Native regressions verify all flag combinations and rollback using a warm parsed document,
validation-cache partitioning in both warming orders, and fresh subscription-variable checks
on cache hits. Further cases verify interface inheritance, union and concrete conditions,
indexed abstract stream children, serial mutation prefixes with lazy deferred fields, and
held child-source cancellation. A two-thread barrier runs 100 cancellation/item-completion
races, checking delivered prefixes and exactly-once closure of every acquired source.

The expanded smoke campaign passed 1,088 exact comparisons over 100 generated inputs and
36 corpus cases, and all 81 incremental Spock regressions passed. The packaging, shaded
consumer, annotation and Javadoc checks also passed. Follow-up campaign and runtime-matrix
measurements are retained separately from the earlier measurements above.

The full expanded nightly campaign completed **50,000 generated inputs and 400,288 exact
Lean comparisons**, plus 1,000 cancellation checks, 3,184 native multi-failure checks,
128 validation pairs and four rejected-item checks. All semantic gates and corruption
sentinels passed. Seed 4294947296 crossed the unsigned boundary; the run contained 44,567
unique IR inputs and 24,969 unique operations, reaching selection depth 10.

New-executor coverage reached **397 of 464 branches (85.6%)** and **870 of 994 lines
(87.5%)**. The production source hash remains unchanged. This campaign expands both the
input domain and sample size, so the gain cannot be attributed to either alone. Its harness
wall time was 1,612.40 seconds while the JVM matrix ran concurrently.

Full Java 11, 17 and 21 Spock suites each ran 5,853 tests with no failures; their expected
skip counts were seven, eight and eight respectively. TestNG passed on Java 11, 17, 21
and 25, each with 141 passed tests and the exact 49 expected skips. The Java 25 focused
suite passed all 81 incremental regressions. Packaging, shaded-consumer, annotation and
Javadoc checks passed. See [follow-up evidence and commands](evidence/pre-push-2026-10-14.json).

## Review-stack isolation measurement

The split keeps defer, its oracle tooling, stream and expanded profiles in separate
branches. Operation setup, field instrumentation and object completion now live in v2
adapters; legacy result implementations are unchanged. The final fuzzing branch changes
verification code only. Historical coverage above therefore has a different denominator.

A fresh CI run at seed `4294967000` crossed the unsigned seed boundary and passed
2,000 generated inputs plus 36 corpus cases: **16,288 exact Lean comparisons**, 100
host checks, 320 native fault checks, 128 validation pairs and four rejected-item checks.
All semantic gates and corruption sentinels passed. V2 execution-package coverage was
**404/492 branches (82.1%)** and **933/1,069 lines (87.3%)**. The measurement used
fuzz execution data only; it does not merge Spock coverage. The Java production hash is
`526f452428acdc5b941f5319d89a0f833c87627098511222605b04de3dbb4928`.
See [fresh source and verification evidence](evidence/review-stack-2026-10-14.json).

```sh
./gradlew incrementalDifferentialFuzz incrementalFuzzCoverageReport \
  -PleanProject=/path/to/graphql-lean -PfuzzProfile=ci -PfuzzCases=2000 \
  -PfuzzSeed=4294967000 -PfuzzCoverage=true
```

This is a bounded CI campaign against the refactored source. The historical 50,000-case
nightly campaign describes the backup source, not this measurement.
