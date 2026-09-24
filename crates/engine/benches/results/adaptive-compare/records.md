# adaptive-compare resource records (task 1.1/1.2)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify | Server E/C/R |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| compare/h1/unshaped/fixed-4 | 2019.43 | 2025.10 | 268435456 | 269189120 | 0 | 0 | 0 | 0.126768288 | 354.97836809155297 | 14940 | 18 | n/r | ok | n/r |
| compare/h1/unshaped/adaptive-1-4 | 784.04 | 784.04 | 268435456 | 268435456 | 0 | 0 | 0 | 0.32651262 | 119.44408151819677 | 14940 | 6 | n/r | ok | n/r |
| compare/h1/shaped/fixed-4 | 48.53 | 48.53 | 268435456 | 268435456 | 0 | 0 | 0 | 5.275176804 | 10.615757932044472 | 14940 | 17 | n/r | ok | n/r |
| compare/h1/shaped/adaptive-1-4 | 36.82 | 37.42 | 268435456 | 272826368 | 0 | 0 | 0 | 6.953354848 | 8.053666355904063 | 14940 | 18 | n/r | ok | n/r |
| compare/h2/unshaped/fixed-4 | 740.74 | 740.74 | 268435456 | 268435456 | 0 | 0 | 0 | 0.345600526 | 127.31462104314042 | 16108 | 27 | n/r | ok | n/r |
| compare/h2/unshaped/adaptive-1-4 | 831.14 | 831.14 | 268435456 | 268435456 | 0 | 0 | 0 | 0.308010705 | 146.09881822126928 | 16140 | 21 | n/r | ok | n/r |
| compare/h2/shaped/fixed-4 | 12.11 | 12.11 | 268435456 | 268435456 | 0 | 0 | 0 | 21.145298814 | 3.357720343634582 | 16192 | 14 | n/r | ok | n/r |
| compare/h2/shaped/adaptive-1-4 | 12.08 | 12.08 | 268435456 | 268435456 | 0 | 0 | 0 | 21.196819525 | 3.538266668334055 | 16220 | 13 | n/r | ok | n/r |
