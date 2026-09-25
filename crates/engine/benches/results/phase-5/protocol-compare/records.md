# Protocol-compare cells (task 5.4)

| Cell | estab H1 | estab H2 | req H1 | H2 streams | CPU % | Peak RSS KiB |
|---|---|---|---|---|---|---|
| protocol/h1/1job/fixed-4/rep0 | 5 | 0 | 18 | 0 | 12.5 | 10484 |
| protocol/h1/1job/fixed-4/rep1 | 5 | 0 | 18 | 0 | 12.9 | 12412 |
| protocol/h1/1job/fixed-4/rep2 | 5 | 0 | 18 | 0 | 12.5 | 12428 |
| protocol/h1/1job/adaptive-1-4/rep0 | 5 | 0 | 20 | 0 | 8.2 | 12212 |
| protocol/h1/1job/adaptive-1-4/rep1 | 5 | 0 | 20 | 0 | 7.4 | 11832 |
| protocol/h1/1job/adaptive-1-4/rep2 | 5 | 0 | 20 | 0 | 8.3 | 12680 |
| protocol/h1/2job/fixed-4/rep0 | 10 | 0 | 36 | 0 | 23.5 | 12892 |
| protocol/h1/2job/fixed-4/rep1 | 10 | 0 | 36 | 0 | 24.2 | 13680 |
| protocol/h1/2job/fixed-4/rep2 | 10 | 0 | 36 | 0 | 24.5 | 13792 |
| protocol/h1/2job/adaptive-1-4/rep0 | 10 | 0 | 40 | 0 | 17.4 | 13200 |
| protocol/h1/2job/adaptive-1-4/rep1 | 10 | 0 | 40 | 0 | 17.0 | 12952 |
| protocol/h1/2job/adaptive-1-4/rep2 | 10 | 0 | 40 | 0 | 15.4 | 13432 |
| protocol/h2/1job/fixed-4/rep0 | 0 | 1 | 0 | 18 | 14.8 | 15136 |
| protocol/h2/1job/fixed-4/rep1 | 0 | 1 | 0 | 18 | 15.6 | 15192 |
| protocol/h2/1job/fixed-4/rep2 | 0 | 1 | 0 | 18 | 16.2 | 15268 |
| protocol/h2/1job/adaptive-1-4/rep0 | 0 | 1 | 0 | 20 | 10.0 | 15288 |
| protocol/h2/1job/adaptive-1-4/rep1 | 0 | 1 | 0 | 21 | 9.8 | 15292 |
| protocol/h2/1job/adaptive-1-4/rep2 | 0 | 1 | 0 | 20 | 9.7 | 15300 |
| protocol/h2/2job/fixed-4/rep0 | 0 | 2 | 0 | 36 | 27.7 | 15472 |
| protocol/h2/2job/fixed-4/rep1 | 0 | 2 | 0 | 36 | 28.0 | 15664 |
| protocol/h2/2job/fixed-4/rep2 | 0 | 2 | 0 | 36 | 28.4 | 15772 |
| protocol/h2/2job/adaptive-1-4/rep0 | 0 | 2 | 0 | 40 | 17.7 | 15852 |
| protocol/h2/2job/adaptive-1-4/rep1 | 0 | 2 | 0 | 40 | 18.6 | 15892 |
| protocol/h2/2job/adaptive-1-4/rep2 | 0 | 2 | 0 | 40 | 18.6 | 15992 |

## Per-job records

| Job | Goodput (MiB/s) | Wall (s) | Completed | Network | Retries | SegReqs | Verify | maxDesired | maxActive | Desired trace | Fallback |
|---|---|---|---|---|---|---|---|---|---|---|
| protocol/h1/1job/fixed-4/rep0/job0 | 48.39 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/1job/fixed-4/rep1/job0 | 48.18 | 2.66 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/1job/fixed-4/rep2/job0 | 48.24 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/1job/adaptive-1-4/rep0/job0 | 30.69 | 4.17 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.06s | none |
| protocol/h1/1job/adaptive-1-4/rep1/job0 | 30.43 | 4.21 | 134217728 | 134283264 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.07s | none |
| protocol/h1/1job/adaptive-1-4/rep2/job0 | 30.64 | 4.18 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.06s | none |
| protocol/h1/2job/fixed-4/rep0/job0 | 48.36 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/2job/fixed-4/rep0/job1 | 48.36 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/2job/fixed-4/rep1/job0 | 48.30 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/2job/fixed-4/rep1/job1 | 48.30 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/2job/fixed-4/rep2/job0 | 48.23 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/2job/fixed-4/rep2/job1 | 48.33 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h1/2job/adaptive-1-4/rep0/job0 | 30.34 | 4.22 | 134217728 | 134250496 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.06s | none |
| protocol/h1/2job/adaptive-1-4/rep0/job1 | 30.34 | 4.22 | 134217728 | 134283158 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.06s | none |
| protocol/h1/2job/adaptive-1-4/rep1/job0 | 30.37 | 4.21 | 134217728 | 134250496 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.04s→4@3.06s | none |
| protocol/h1/2job/adaptive-1-4/rep1/job1 | 30.26 | 4.23 | 134217728 | 134283211 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.04s→4@3.06s | none |
| protocol/h1/2job/adaptive-1-4/rep2/job0 | 30.62 | 4.18 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.07s | none |
| protocol/h1/2job/adaptive-1-4/rep2/job1 | 30.43 | 4.21 | 134217728 | 134283158 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.07s | none |
| protocol/h2/1job/fixed-4/rep0/job0 | 47.64 | 2.69 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/1job/fixed-4/rep1/job0 | 47.61 | 2.69 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/1job/fixed-4/rep2/job0 | 48.42 | 2.64 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/1job/adaptive-1-4/rep0/job0 | 30.49 | 4.20 | 134217728 | 134225920 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.04s→4@3.06s | none |
| protocol/h2/1job/adaptive-1-4/rep1/job0 | 30.51 | 4.20 | 134217728 | 134217728 | 0 | 19 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.04s→4@3.06s | none |
| protocol/h2/1job/adaptive-1-4/rep2/job0 | 30.67 | 4.17 | 134217728 | 134225920 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.04s→4@3.06s | none |
| protocol/h2/2job/fixed-4/rep0/job0 | 48.19 | 2.66 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/2job/fixed-4/rep0/job1 | 47.46 | 2.70 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/2job/fixed-4/rep1/job0 | 47.68 | 2.68 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/2job/fixed-4/rep1/job1 | 48.41 | 2.64 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/2job/fixed-4/rep2/job0 | 48.32 | 2.65 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/2job/fixed-4/rep2/job1 | 47.59 | 2.69 | 134217728 | 134217728 | 0 | 16 | published=true size_ok=true hash_ok=true | 4 | 4 | 4@0.05s | none |
| protocol/h2/2job/adaptive-1-4/rep0/job0 | 30.69 | 4.17 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.05s→4@3.07s | none |
| protocol/h2/2job/adaptive-1-4/rep0/job1 | 29.36 | 4.36 | 134217728 | 136183808 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.02s→3@2.05s→4@3.07s | none |
| protocol/h2/2job/adaptive-1-4/rep1/job0 | 30.65 | 4.18 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.09s→4@3.11s | none |
| protocol/h2/2job/adaptive-1-4/rep1/job1 | 30.33 | 4.22 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.09s→4@3.11s | none |
| protocol/h2/2job/adaptive-1-4/rep2/job0 | 30.64 | 4.18 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.09s→4@3.06s | none |
| protocol/h2/2job/adaptive-1-4/rep2/job1 | 30.34 | 4.22 | 134217728 | 134217728 | 0 | 18 | published=true size_ok=true hash_ok=true | 4 | 4 | 1@0.05s→2@1.07s→3@2.09s→4@3.06s | none |
