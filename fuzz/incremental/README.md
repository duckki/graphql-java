# Defer differential fuzzing

This optional task compares the defer-only v2 executor against the supplied Lean
alternative model, pinned to `aa3f4ee870c7b865b81720aa672d9052a27b3695`.
The adapter imports the unchanged executor and work queue, including ordered group
declarations, delivery-cause projections and queued startup-failure notices. Lean runs eagerly with a
finite admissible task-first schedule. Java runs independently through its native API.

```sh
./gradlew incrementalDifferentialFuzz \
  -PleanProject=/path/to/graphql-lean -PfuzzCases=1000 -PfuzzSeed=1
```

The model checkout must have no tracked changes and have its Lean dependencies built.
The task is separate from ordinary tests, reuses test-runtime Jackson, and introduces
no dependencies. `-PleanOracle=/path/to/compiled-oracle` selects a compiled adapter;
otherwise `lake env lean --run` executes the checked-in source.

Each input runs with synchronous and completed-future resolvers, inline and named
fragments, and rotating one-item, burst-three and unbounded publisher demand. The
seeded generator uses `SplittableRandom`, a 36-selection budget, depth three and lists
up to three elements. Cases include shared owners, deep ancestry, conditions, aliases,
nullable resolver errors and single-origin fatal defer errors. Sixteen saved cases
cover additional ownership and interface behavior. Generated non-null fields stay
healthy except in the dedicated single-failure family.

The comparator checks exact initial data and error count, then exact reconstructed
final data and total error count. A separate observer checks delivery IDs, paths,
completion and terminal flags. Deliberately corrupted data, error counts and completion
IDs must be detected. Agreement covers this finite domain; it is not a formal Java
refinement proof or a check of all asynchronous schedules.

Failure artifacts retain original and minimized inputs and observations. Shrinking
preserves the diagnostic category and tries at most 100 selection deletions. Replay
restores the saved resolver, document and demand options:

```sh
./gradlew incrementalDifferentialFuzz \
  -PleanProject=/path/to/graphql-lean -PfuzzReplay=build/incremental-fuzz/failures/example.json
```

Options: `fuzzCases`, `fuzzSeed`, `fuzzCorpus`, `fuzzOutput`, `fuzzReplay`,
`fuzzResolvers=sync,promise`, `fuzzDocuments=inline,named`,
`fuzzDemands=one,burst,unbounded`, and `fuzzSentinel=true`.
The report records the model pin, source hashes, seed and exact comparison count.
Stream cases, delayed schedules, host properties and coverage profiles are later stages.
