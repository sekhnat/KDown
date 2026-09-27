# Release performance suite

- Profile: `wan` v1; dataset 8 MiB; RTT 80.0 ms; jitter 20 ms; loss 1.0%; bandwidth 2.5
- Fingerprint: `b5fd744c9cd45793` (disk `tmpfs`); repetitions 5; generated 2026-09-27T10:45:27Z


| Scenario | Throughput MiB/s (median) | Spread % | Amplification | CPU ns/B | Managed high-water B | Cap B | Verify |
|
|---|---|---|---|---|---|---|---|
| wan/h1/workers_1/jobs_1 | 2.367 | unavailable | 1.000 | 1.192 | 65536 | 67108864 | ok |
| wan/h1/workers_4/jobs_1 | 2.298 | unavailable | 1.007 | 2.384 | 61440 | 67108864 | ok |
| wan/h2/workers_1/jobs_1 | 2.392 | unavailable | 1.000 | 2.384 | 4096 | 67108864 | ok |
| wan/h2/workers_4/jobs_1 | 2.317 | unavailable | 1.009 | 4.768 | 16384 | 67108864 | ok |

## Validation

Every mandatory axis has a measured value and every run verified.
