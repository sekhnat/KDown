# Buffer-sweep cells (task 7.4)

| Cell | read_buffer KiB | Goodput (MiB/s) | Wall (s) | CPU % | ctx/s | Peak RSS KiB | Retries | SegReqs | Verify |
|---|---|---|---|---|---|---|---|---|---|
| buffer/h1/lan/64k/rep0 | 64 | 2775.05 | 0.02 | 520.3 | 650 | 10228 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/64k/rep1 | 64 | 1009.57 | 0.06 | 220.8 | 205 | 11248 | 0 | 10 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/64k/rep2 | 64 | 2921.10 | 0.02 | 639.0 | 821 | 12224 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/128k/rep0 | 128 | 981.92 | 0.07 | 214.8 | 260 | 15816 | 0 | 10 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/128k/rep1 | 128 | 2787.55 | 0.02 | 609.8 | 130 | 15816 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/128k/rep2 | 128 | 2779.37 | 0.02 | 608.0 | 477 | 15816 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/256k/rep0 | 256 | 1008.18 | 0.06 | 220.5 | 47 | 15816 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/256k/rep1 | 256 | 1001.79 | 0.06 | 234.8 | 46 | 17096 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/256k/rep2 | 256 | 1022.00 | 0.06 | 223.6 | 31 | 17096 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/512k/rep0 | 512 | 984.86 | 0.06 | 215.4 | 230 | 17096 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/512k/rep1 | 512 | 1021.21 | 0.06 | 223.4 | 63 | 17096 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h1/lan/512k/rep2 | 512 | 2804.95 | 0.02 | 569.8 | 394 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/64k/rep0 | 64 | 48.17 | 1.33 | 12.0 | 2 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/64k/rep1 | 64 | 48.17 | 1.33 | 12.0 | 1 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/64k/rep2 | 64 | 48.28 | 1.33 | 12.1 | 1 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/128k/rep0 | 128 | 48.32 | 1.32 | 12.1 | 15 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/128k/rep1 | 128 | 48.23 | 1.33 | 12.1 | 1 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/128k/rep2 | 128 | 48.44 | 1.32 | 12.1 | 0 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/256k/rep0 | 256 | 48.20 | 1.33 | 11.3 | 3 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/256k/rep1 | 256 | 48.35 | 1.32 | 13.6 | 3 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/256k/rep2 | 256 | 48.40 | 1.32 | 12.1 | 2 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/512k/rep0 | 512 | 48.31 | 1.32 | 12.8 | 1 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/512k/rep1 | 512 | 48.09 | 1.33 | 12.0 | 3 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h1/wan/512k/rep2 | 512 | 48.10 | 1.33 | 12.0 | 11 | 17096 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/64k/rep0 | 64 | 2478.63 | 0.03 | 697.1 | 77 | 17492 | 0 | 10 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/64k/rep1 | 64 | 2595.65 | 0.02 | 730.0 | 40 | 17524 | 0 | 11 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/64k/rep2 | 64 | 983.25 | 0.07 | 261.2 | 30 | 17988 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/128k/rep0 | 128 | 2573.62 | 0.02 | 683.6 | 241 | 17988 | 0 | 11 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/128k/rep1 | 128 | 965.52 | 0.07 | 256.5 | 15 | 17988 | 0 | 11 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/128k/rep2 | 128 | 2692.44 | 0.02 | 715.2 | 42 | 17988 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/256k/rep0 | 256 | 2523.66 | 0.03 | 670.3 | 39 | 17988 | 0 | 13 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/256k/rep1 | 256 | 2471.44 | 0.03 | 695.1 | 386 | 17988 | 0 | 11 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/256k/rep2 | 256 | 2578.12 | 0.02 | 684.8 | 201 | 18068 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/512k/rep0 | 512 | 2631.74 | 0.02 | 740.2 | 123 | 17988 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/512k/rep1 | 512 | 2477.56 | 0.03 | 696.8 | 232 | 17988 | 0 | 10 | published=true size_ok=true hash_ok=true |
| buffer/h2/lan/512k/rep2 | 512 | 972.74 | 0.07 | 273.6 | 182 | 19412 | 0 | 9 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/64k/rep0 | 64 | 45.38 | 1.41 | 14.9 | 1 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/64k/rep1 | 64 | 46.91 | 1.36 | 14.7 | 1 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/64k/rep2 | 64 | 46.90 | 1.36 | 14.7 | 0 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/128k/rep0 | 128 | 45.57 | 1.40 | 15.0 | 1 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/128k/rep1 | 128 | 46.85 | 1.37 | 14.6 | 1 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/128k/rep2 | 128 | 46.59 | 1.37 | 16.0 | 2 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/256k/rep0 | 256 | 48.00 | 1.33 | 15.8 | 0 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/256k/rep1 | 256 | 48.26 | 1.33 | 16.6 | 1 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/256k/rep2 | 256 | 48.32 | 1.32 | 16.6 | 0 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/512k/rep0 | 512 | 46.89 | 1.36 | 15.4 | 2 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/512k/rep1 | 512 | 46.81 | 1.37 | 15.4 | 0 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |
| buffer/h2/wan/512k/rep2 | 512 | 46.70 | 1.37 | 15.3 | 7 | 19412 | 0 | 8 | published=true size_ok=true hash_ok=true |

Unavailable axes (labeled, not fabricated): syscall counts and allocation counts are not
instrumented; context switches (in+vol, client process) stand in as scheduling-pressure proxy;
peak RSS is the monotonic process VmHWM (coarse per-cell caveat).
