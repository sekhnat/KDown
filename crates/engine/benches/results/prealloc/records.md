# prealloc resource records (task 1.1/1.2)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify | Server E/C/R |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prealloc_true | 3150.30 | 3954.10 | 33554432 | 42115809 | 0 | 262144 | 0 | 0.010157747 | 393.7881106902938 | 71656 | 2 | n/r | ok | n/r  | n/r |
| prealloc_false | 2513.73 | 3089.93 | 33554432 | 41245923 | 0 | 375772 | 0 | 0.012730106 | 314.21576536754685 | 73840 | 4 | n/r | ok | n/r  | n/r |
