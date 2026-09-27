# Release performance suite

- Profile: `loopback` v1; dataset 64 MiB; RTT 0.0 ms; jitter 0 ms; loss 0.0%; bandwidth null
- Fingerprint: `33919309d07fed4f` (disk `tmpfs`); repetitions 7; generated 2026-09-27T10:44:02Z


| Scenario | Throughput MiB/s (median) | Spread % | Amplification | CPU ns/B | Managed high-water B | Cap B | Verify |
|
|---|---|---|---|---|---|---|---|
| loopback/h1/workers_1/jobs_1 | 412.458 | unavailable | 1.000 | 0.745 | 163893 | 67108864 | ok |
| loopback/h1/workers_1/jobs_4 | 835.832 | unavailable | 1.000 | 0.708 | 196502 | 67108864 | ok |
| loopback/h1/workers_4/jobs_1 | 440.675 | unavailable | 1.003 | 1.043 | 299008 | 67108864 | ok |
| loopback/h1/workers_4/jobs_4 | 824.199 | unavailable | 1.003 | 1.118 | 131072 | 67108864 | ok |
| loopback/h1/workers_8/jobs_1 | 540.684 | unavailable | 1.004 | 1.341 | 372630 | 67108864 | ok |
| loopback/h1/workers_8/jobs_4 | 762.197 | unavailable | 1.002 | 1.341 | 245707 | 67108864 | ok |
| loopback/h2/workers_1/jobs_1 | 326.157 | unavailable | 1.000 | 1.043 | 4096 | 67108864 | ok |
| loopback/h2/workers_1/jobs_4 | 790.596 | unavailable | 1.000 | 1.080 | 4096 | 67108864 | ok |
| loopback/h2/workers_4/jobs_1 | 465.376 | unavailable | 1.003 | 2.384 | 16384 | 67108864 | ok |
| loopback/h2/workers_4/jobs_4 | 677.824 | unavailable | 1.002 | 2.235 | 16384 | 67108864 | ok |
| loopback/h2/workers_8/jobs_1 | 464.912 | unavailable | 1.003 | 2.831 | 32768 | 67108864 | ok |
| loopback/h2/workers_8/jobs_4 | 669.175 | unavailable | 1.002 | 2.570 | 32768 | 67108864 | ok |

## Validation

Every mandatory axis has a measured value and every run verified.
