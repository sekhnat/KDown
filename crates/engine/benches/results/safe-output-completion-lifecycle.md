# Safe-output lifecycle benchmark record

## Pre-change baseline

Recorded 2026-09-23 on the same host intended for the post-change run. The measured transfer/output implementation was at repository revision `a449f7d`; only the unrelated rustdoc-link correction had been made. Fixture and harness: 32 MiB, four workers, Criterion with 10 samples, 0.5 s warm-up and 1.5 s measurement, localhost raw TCP HTTP/1.1 and localhost HTTPS/HTTP/2 ALPN, default benchmark profile. The benchmark process was run once per protocol, sequentially.

| Protocol | Criterion time (95% interval; median) | Throughput (95% interval; median) | Peak RSS |
|---|---:|---:|---:|
| HTTP/1.1, workers=4 | 17.288–17.450 ms; 17.372 ms | 1.7909–1.8076 GiB/s; 1.7989 GiB/s | 89,560 KiB (87.5 MiB) |
| HTTP/2, workers=4 | 16.766–19.218 ms; 17.796 ms | 1.6261–1.8639 GiB/s; 1.7560 GiB/s | 80,536 KiB (78.6 MiB) |

Peak RSS is the benchmark subprocess high-water mark reported by Linux `getrusage(RUSAGE_CHILDREN).ru_maxrss` (KiB), collected in a small Python wrapper around each isolated Criterion process. This measures the harness process, including its fixture server and 32 MiB fixture, not just the engine's incremental allocation. The HTTP/2 interval was notably wider; treat single-run values as a baseline, not a claim of a performance difference. Gnuplot was unavailable, so Criterion used its plotters backend.

## Host and toolchain

- AMD Ryzen 7 9700X, 8 cores / 16 threads; 31 GiB RAM (18.7 GiB available before measurement).
- CachyOS Linux x86_64, kernel `7.2.6-1-cachyos`.
- `rustc 1.98.1 (48a229cea 2026-09-01)`, `cargo 1.98.1`.
- Commit: `a449f7d`.

## Reproduction for the post-change run

Build the optimized harness once, then run each protocol in a separate process and in sequence. `--bench` is required when invoking the Criterion binary directly. Resolve the executable after the build because Cargo may change its hash:

```sh
cargo bench --bench throughput --no-run
BENCH="$(find target/release/deps -maxdepth 1 -type f -executable -name 'throughput-*' | head -n 1)"

# Repeat for h1/workers_4 and h2/workers_4; record Criterion's time and
# throughput intervals plus the printed peak_rss_kib for each invocation.
python3 -c 'import resource, subprocess, sys; p=subprocess.run(sys.argv[1:]); u=resource.getrusage(resource.RUSAGE_CHILDREN); print(f"peak_rss_kib={u.ru_maxrss}", file=sys.stderr); sys.exit(p.returncode)' \
  "$BENCH" --bench 'h1/workers_4' --warm-up-time 0.5 --measurement-time 1.5 --sample-size 10
python3 -c 'import resource, subprocess, sys; p=subprocess.run(sys.argv[1:]); u=resource.getrusage(resource.RUSAGE_CHILDREN); print(f"peak_rss_kib={u.ru_maxrss}", file=sys.stderr); sys.exit(p.returncode)' \
  "$BENCH" --bench 'h2/workers_4' --warm-up-time 0.5 --measurement-time 1.5 --sample-size 10
```

Use the same host, fixture/harness, toolchain, arguments, and measurement method for the post-change comparison. Record any host-load or toolchain differences alongside the new values.

## Post-change results

Recorded 2026-09-23 on the same AMD Ryzen 7 9700X / CachyOS Linux host and Rust 1.98.1 toolchain as the baseline. The harness used the same 32 MiB fixture, four workers, 10 Criterion samples, 0.5 s warm-up, 1.5 s measurement, and sequential isolated processes. `getrusage(RUSAGE_CHILDREN).ru_maxrss` was collected with the same Python wrapper. At 23:19 +05:00, 15 GiB RAM was available and load averages were 0.72 / 0.89 / 0.81. Gnuplot remained unavailable, so Criterion used plotters.

| Protocol | Time (95% interval; median) | Throughput (95% interval; median) | Peak RSS |
|---|---:|---:|---:|
| HTTP/1.1, workers=4 | 16.669–16.955 ms; 16.846 ms | 1.8431–1.8747 GiB/s; 1.8551 GiB/s | 95,736 KiB (93.5 MiB) |
| HTTP/2, workers=4 | 15.762–16.388 ms; 16.126 ms | 1.9068–1.9826 GiB/s; 1.9379 GiB/s | 81,032 KiB (79.1 MiB) |

### Before/after comparison

| Protocol | Throughput: baseline → post | Median change | Peak RSS: baseline → post | RSS change |
|---|---:|---:|---:|---:|
| HTTP/1.1, workers=4 | 1.7989 → 1.8551 GiB/s | +3.1% | 89,560 → 95,736 KiB | +6,176 KiB (+6.0 MiB) |
| HTTP/2, workers=4 | 1.7560 → 1.9379 GiB/s | +10.4% | 80,536 → 81,032 KiB | +496 KiB (+0.5 MiB) |

No material throughput regression was observed. Both medians improved; RSS growth is small and below the repository's 64 MiB memory-regression threshold. These are single post-change runs, so they document comparability rather than a general performance claim.
