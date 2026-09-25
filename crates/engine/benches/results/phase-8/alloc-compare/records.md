# Alloc-compare cells (task 8.1)

| Cell | physical | first byte (ms) | Goodput (MiB/s) | Wall (s) | CPU % | Verify |
|---|---|---|---|---|---|---|
| alloc/tmpfs/logical/rep0 | false | 14 | 2048.18 | 0.12 | 456.0 | published=true size_ok=true hash_ok=true |
| alloc/tmpfs/logical/rep1 | false | 14 | 3082.18 | 0.08 | 674.2 | published=true size_ok=true hash_ok=true |
| alloc/tmpfs/logical/rep2 | false | 14 | 3027.63 | 0.08 | 674.1 | published=true size_ok=true hash_ok=true |
| alloc/tmpfs/physical/rep0 | true | 43 | 1694.54 | 0.15 | 364.1 | published=true size_ok=true hash_ok=true |
| alloc/tmpfs/physical/rep1 | true | 46 | 2262.90 | 0.11 | 495.0 | published=true size_ok=true hash_ok=true |
| alloc/tmpfs/physical/rep2 | true | 44 | 2261.69 | 0.11 | 477.1 | published=true size_ok=true hash_ok=true |
| alloc/project-fs/logical/rep0 | false | 14 | 3095.69 | 0.08 | 665.1 | published=true size_ok=true hash_ok=true |
| alloc/project-fs/logical/rep1 | false | 14 | 3082.19 | 0.08 | 662.2 | published=true size_ok=true hash_ok=true |
| alloc/project-fs/logical/rep2 | false | 15 | 2009.52 | 0.13 | 439.6 | published=true size_ok=true hash_ok=true |
| alloc/project-fs/physical/rep0 | true | 41 | 1693.01 | 0.15 | 350.5 | published=true size_ok=true hash_ok=true |
| alloc/project-fs/physical/rep1 | true | 42 | 2218.36 | 0.12 | 485.3 | published=true size_ok=true hash_ok=true |
| alloc/project-fs/physical/rep2 | true | 44 | 1698.48 | 0.15 | 358.3 | published=true size_ok=true hash_ok=true |

Unavailable axes (labeled, not fabricated): fragmentation (FIEMAP extents) and device-level
ENOSPC injection are not plumbed; ENOSPC/permission surfacing is covered by sink error-path tests.
Cancellation cleanup is covered by the suites re-run in task 8.2, not re-measured here.
