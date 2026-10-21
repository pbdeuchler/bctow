# Performance measurements and remaining tradeoffs

The October 2026 changes reduce shared writes, hashing, and division on the
existing API's hot paths. The single-producer contract, arbitrary ring capacities,
overflow policies, subscription order, and allocation-free steady state remain intact.

## Measurements

Measured on an Apple M1 Max with 10 physical cores, using rustc 1.95.0 and LLVM
22.1.2. Synchronous Criterion cases used 40 samples; concurrent cases used 20.
Each used one second of warmup and a two-second measurement target. Processes were
not pinned to cores. Results describe these workloads on this machine.

The baseline is commit `c519036`. Original and optimized code were built in
separate target directories with identical measured benchmark fixtures. Runs were
serialized. Tables use Criterion's slope estimate for linear sampling and mean
for flat sampling; the
[results file](performance-results.json) preserves estimates and 95% confidence intervals.

| Default features | Original | Optimized | Speedup |
| --- | ---: | ---: | ---: |
| Publish by cached ID | 11.18 ns | 7.45 ns | 1.50x |
| Publish by string | 30.02 ns | 26.60 ns | 1.13x |
| Publish and poll, `u64` | 29.97 ns | 16.24 ns | 1.85x |
| Empty poll, one subscription | 21.71 ns | 1.89 ns | 11.50x |
| Idle scan, 1,024 subscriptions | 17.16 us | 1.23 us | 13.96x |
| Idle scan, 10,000 subscriptions | 168.24 us | 13.02 us | 12.92x |
| Last topic active, 10,000 subscriptions | 170.78 us | 12.63 us | 13.52x |
| Publish and poll, `FixedBytes<64>` | 30.65 ns | 17.61 ns | 1.74x |
| Publish and poll, `FixedBytes<512>` | 67.00 ns | 33.90 ns | 1.98x |
| Publish and poll, `FixedBytes<4096>` | 199.65 ns | 174.68 ns | 1.14x |

With the optional `metrics` feature:

| Operation | Original | Optimized | Speedup |
| --- | ---: | ---: | ---: |
| Publish by cached ID | 12.06 ns | 7.60 ns | 1.59x |
| Publish and poll, `u64` | 30.04 ns | 17.45 ns | 1.72x |
| Empty poll | 22.08 ns | 1.89 ns | 11.66x |
| Publish and poll, `FixedBytes<4096>` | 206.27 ns | 175.83 ns | 1.17x |

The sparse fixture keeps earlier subscriptions idle and publishes to the last
one, forcing a full scan for each successful receive. The separate topic-scaling
workload keeps every topic active and normally succeeds on its first candidate;
it measures a different working set. Round-trip measurements include publication
and should not be interpreted as isolated receive latency.

### Concurrent broadcasts

Each batch publishes 65,536 `u64` messages after releasing one, two, or four
consumer workers. Setup is outside the timer; publication, completion, joining,
and teardown are timed. The retained-batch ring holds every message and asserts
that all consumers receive the complete batch.

| Consumers | Original, default | Optimized, default | Original, metrics | Optimized, metrics |
| --- | ---: | ---: | ---: | ---: |
| 1 | 2.319 ms | 1.965 ms | 2.630 ms | 1.290 ms |
| 2 | 3.810 ms | 1.606 ms | 6.710 ms | 3.609 ms |
| 4 | 8.338 ms | 6.799 ms | 18.431 ms | 15.854 ms |

The overwrite fixture uses only 64 slots and ends with a terminal marker. Its
reported throughput counts publications, not deliveries. Different loss rates
mean completion times do not represent equal amounts of delivery work:

| Consumers | Original, default | Optimized, default | Original, metrics | Optimized, metrics |
| --- | ---: | ---: | ---: | ---: |
| 1 | 4.171 ms | 4.163 ms | 4.399 ms | 3.864 ms |
| 2 | 6.422 ms | 2.828 ms | 8.664 ms | 6.517 ms |
| 4 | 18.248 ms | 18.715 ms | 21.764 ms | 22.972 ms |

With one consumer and default features, observed deliveries rose from about
37,071 to 65,456 per batch. With four consumers and metrics, they rose from about
176,081 to 205,404 while completion took longer. Delivery observations include
calibration and warmup batches; they are not paired per-sample rate estimates.
These results show why lossy publication time alone cannot establish a universal
performance improvement.

The harness accumulates delivery counts locally and combines them once per
worker. A shared per-delivery harness counter was removed before these concurrent
comparisons because it introduced unrelated contention. The optional library
delivery metric remains enabled in the metrics measurements. Heavily
oversubscribed stress runs verify completion; their variable timings are not used
to claim throughput gains.

## Retained changes

- Read atomic slot metadata before admission to avoid reader-count writes on
  unavailable sequences. Recheck the sequence after admission for correctness.
- Advance the producer's sequence with checked load/store operations. Only one
  producer writes it; reservation still occurs before the payload copy.
- Mask power-of-two indices and retain full-width modulo for other capacities.
  No capacity restriction or extra per-ring metadata was added.
- Scan a dense, ordered subscription vector. Keep ID lookup in a separate index
  map for subscription changes. Subscribe remains expected O(1); unsubscribe
  remains O(n), with additional cold-path index updates after compaction.
- Check the ring header before touching an idle subscription's slot allocation.
- Skip a redundant writer-state load when the ownership RMW reports no readers.
  Contended writers still wait for every admitted reader.
- Update publication metrics with wrapping atomic load/store operations under
  the single-writer invariant. Delivery metrics retain their multiwriter RMW.

## Research and stopping point

The [LMAX Disruptor technical report](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)
motivates single-writer ownership, reducing coordination, batching, and cache
locality. Its [gating protocol](https://lmax-exchange.github.io/disruptor/user-guide/)
prevents producers from overrunning consumers. Applying its reader protocol here
would change lossy overwrite behavior, so the payload exclusion protocol remains.

[FastForward](https://liberty.princeton.edu/Publications/pact07_ff.pdf) and
[Torquati's SPSC study](https://arxiv.org/abs/1012.1824) examine communication and
cache costs in single-producer/single-consumer queues. Their results motivate
avoiding unnecessary shared writes and checking different occupancy regimes.
Applying those ideas to this broadcast broker is an inference; their queue
algorithms are not replacements for its arbitrary `Copy` payload protocol.

[Nikolaev's SCQ paper, DISC 2019](https://arxiv.org/html/1908.04511v1) studies
CAS contention, fetch-and-add, and cache-aware entry placement. SCQ distributes
each item to one consumer. This broker broadcasts items, and reader admission
must respect writer ownership and count saturation. Replacing its CAS with an
unchecked increment would not preserve those conditions.

[Rigtorp's SPSC implementation study](https://rigtorp.se/ringbuffer/) caches the
opposing index to reduce shared reads. A similar per-cursor frontier was tested
here, with slot admission unchanged. It added eight bytes per cursor, slowed the
10,000-topic idle scan to about 15 us, and did not improve the sampled concurrent
batches. That trial was discarded rather than generalizing SPSC results to this
broadcast design.

The remaining candidates have explicit costs:

| Candidate | Why it was rejected or deferred |
| --- | --- |
| Resolve string names to IDs, then use the cache | Three variants, including inlining and a shared cache helper, measured about 29.3, 30.6, and 33.3 ns versus 27.3 ns for the existing path on the optimized ring. All were discarded. |
| Cache each cursor's observed writer position | Added memory and local state without measured benefit in this trial; discarded. |
| Pad or remap slots | Changes memory footprint or sequential locality. Padding a 24-byte `u64` slot to 128 bytes would exceed five times its storage. Results would need workload and hardware-specific evaluation. |
| Shard delivery metrics | Adds per-consumer storage, lifecycle handling, and snapshot aggregation. The exact shared delivery counter remains a real contention point. |
| Add per-consumer progress gates or hazards | Adds registration and reclamation protocols; gating also introduces backpressure. |
| Add batch APIs | Could amortize synchronization, but changes API, latency, and fairness choices. |
| Track ready subscriptions | Could avoid O(n) sparse scans, but adds publisher-side writes and concurrent subscription lifecycle state. |
| Change spin/backoff policy | Trades CPU consumption against latency and needs scheduling-specific workloads. |
| Remove checked cache borrowing or use post-copy validation | Adds unsafe assumptions; retrying a raced non-atomic payload copy cannot make it sound. |

These changes exhaust the identified small improvements without adding an API or
a new ownership protocol. Further work should start from a specific workload and
an explicit choice among the remaining tradeoffs.

## Reproduction

Run the focused suite with metrics disabled, then enabled:

```sh
cargo bench --bench benchmarks -- 'operations|subscription_scan|message_size|ring_capacity' \
  --sample-size 40 --warm-up-time 1 --measurement-time 2 --save-baseline measured
cargo bench --all-features --bench benchmarks -- 'operations|message_size' \
  --sample-size 40 --warm-up-time 1 --measurement-time 2 --save-baseline measured_metrics
cargo bench --bench benchmarks -- broadcast \
  --sample-size 20 --warm-up-time 1 --measurement-time 2 --save-baseline concurrent
cargo bench --all-features --bench benchmarks -- broadcast \
  --sample-size 20 --warm-up-time 1 --measurement-time 2 --save-baseline concurrent_metrics
```

For a historical comparison, export `c519036` to a separate directory, copy
`benches/benchmarks.rs` and `benches/support/harness.rs` into that export, and use
its own target directory. Do not
share `CARGO_TARGET_DIR` between the two copies: identical package names and old
archive timestamps can allow stale artifacts to be reused. Keep measurements
serial and report feature flags, payload sizes, and occupancy with the results.
