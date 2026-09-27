# prealloc resource records (task 1.1/1.2)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify | Server E/C/R |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prealloc_true | 2895.46 | 3534.25 | 33554432 | 40957228 | 0 | 549676 | 0 | 0.0110518 | 361.9319929785193 | 57496 | 5 | n/r | ok | n/r  | n/r |
| prealloc_false | 2791.71 | 3237.83 | 33554432 | 38916388 | 0 | 645014 | 0 | 0.011462493 | 436.2052827425936 | 57688 | 4 | n/r | ok | n/r  | n/r |
