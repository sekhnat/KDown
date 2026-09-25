# Origin-compare cells (task 6.5)

| Cell | Job goodput (MiB/s) | Job wall (s) | Aggregate (MiB/s) | 503s | Requests | Connections | Registry thr/ok | Verify |
|---|---|---|---|---|---|---|---|---|
| origin/shared/same/1job/rep0 | 62.54 | 1.02 | 60.79 | 1 | 11 | 5 | 1/9 | published=true size_ok=true hash_ok=true |
| origin/shared/same/1job/rep1 | 60.12 | 1.06 | 58.49 | 1 | 12 | 5 | 1/10 | published=true size_ok=true hash_ok=true |
| origin/shared/same/1job/rep2 | 60.10 | 1.06 | 58.50 | 1 | 12 | 5 | 1/10 | published=true size_ok=true hash_ok=true |
| origin/shared/same/2job/rep0 | 59.80/59.69 | 1.07/1.07 | 113.45 | 1 | 28 | 10 | 2/50 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.998
| origin/shared/same/2job/rep1 | 62.09/62.16 | 1.03/1.03 | 117.57 | 1 | 28 | 9 | 2/50 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.999
| origin/shared/same/2job/rep2 | 59.74/62.22 | 1.07/1.03 | 113.37 | 1 | 27 | 10 | 2/48 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.960
| origin/shared/mixed/2job/rep0 | 59.58/61.91 | 1.07/1.03 | 113.11 | 2 | 29 | 10 | 2/25 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.962
| origin/shared/mixed/2job/rep1 | 62.00/59.65 | 1.03/1.07 | 116.14 | 2 | 28 | 10 | 2/24 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.962
| origin/shared/mixed/2job/rep2 | 61.90/59.54 | 1.03/1.07 | 115.94 | 2 | 31 | 12 | 2/27 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.962
| origin/fallback/same/1job/rep0 | 60.07 | 1.07 | 58.45 | 1 | 12 | 5 | 0/0 | published=true size_ok=true hash_ok=true |
| origin/fallback/same/1job/rep1 | 62.40 | 1.03 | 60.66 | 1 | 13 | 5 | 0/0 | published=true size_ok=true hash_ok=true |
| origin/fallback/same/1job/rep2 | 62.48 | 1.02 | 60.77 | 1 | 13 | 5 | 0/0 | published=true size_ok=true hash_ok=true |
| origin/fallback/same/2job/rep0 | 2696.30/62.45 | 0.02/1.02 | 121.51 | 1 | 23 | 8 | 0/0 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.023
| origin/fallback/same/2job/rep1 | 1013.74/62.36 | 0.06/1.03 | 121.29 | 1 | 24 | 8 | 0/0 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.062
| origin/fallback/same/2job/rep2 | 2827.71/59.83 | 0.02/1.07 | 116.52 | 1 | 22 | 6 | 0/0 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.021
| origin/fallback/mixed/2job/rep0 | 59.73/62.22 | 1.07/1.03 | 113.39 | 2 | 29 | 12 | 0/0 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.960
| origin/fallback/mixed/2job/rep1 | 62.08/62.07 | 1.03/1.03 | 117.49 | 2 | 30 | 10 | 0/0 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 1.000
| origin/fallback/mixed/2job/rep2 | 59.54/59.59 | 1.07/1.07 | 113.02 | 2 | 29 | 10 | 0/0 | published=true size_ok=true hash_ok=true ; published=true size_ok=true hash_ok=true | 0.999
