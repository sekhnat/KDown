# Release performance suite

- Profile: `low-latency` v1; dataset 8 MiB; RTT 10.0 ms; jitter 1 ms; loss 0.1%; bandwidth 12.5
- Fingerprint: `8fdeeeddb03afdf5` (disk `tmpfs`); repetitions 5; generated 2026-09-27T10:44:17Z


| Scenario | Throughput MiB/s (median) | Spread % | Amplification | CPU ns/B | Managed high-water B | Cap B | Verify |
|
|---|---|---|---|---|---|---|---|
| low-latency/h1/workers_1/jobs_1 | 11.393 | unavailable | 1.000 | 1.192 | 65536 | 67108864 | ok |
| low-latency/h1/workers_4/jobs_1 | 11.592 | unavailable | 1.011 | 2.384 | 65483 | 67108864 | ok |
| low-latency/h2/workers_1/jobs_1 | 10.978 | unavailable | 1.000 | 2.384 | 4096 | 67108864 | ok |
| low-latency/h2/workers_4/jobs_1 | 11.250 | unavailable | 1.015 | 3.576 | 16384 | 67108864 | ok |

## Validation

Every mandatory axis has a measured value and every run verified.
