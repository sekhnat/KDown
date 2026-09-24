# prealloc resource records (task 1.1/1.2)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify | Server E/C/R |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prealloc_true | 2431.67 | 4220.11 | 33554432 | 58232902 | 0 | 0 | 0 | 0.013159671 | 303.9589667553239 | 74816 | 2 | n/r | ok | n/r |
| prealloc_false | 2607.47 | 4179.30 | 33554432 | 53781623 | 0 | 0 | 0 | 0.01227244 | 407.4169439817998 | 77616 | 2 | n/r | ok | n/r |
