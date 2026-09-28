# prealloc resource records (task 1.1/1.2)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify | Server E/C/R |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prealloc_true | 2431.87 | 2609.28 | 33554432 | 36002330 | 0 | 322454 | 0 | 0.013158593 | 303.98386818408324 | 56552 | 6 | n/r | ok | n/r  | n/r |
| prealloc_false | 2330.43 | 2615.66 | 33554432 | 37661271 | 0 | 244240 | 0 | 0.013731354 | 291.30412048221905 | 56452 | 17 | n/r | ok | n/r  | n/r |
