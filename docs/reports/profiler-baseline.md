# atomr profiler — cross-runtime comparison

- **rust** v0.1.0 host=`linux/aarch64 cpus=20`
- **python** v0.1.0 host=`linux/aarch64 cpus=20 py=3.12.3`

| scenario | runtime | config | msgs | elapsed | throughput | p50 | p95 | p99 | ΔRSS | CPU |
|---|---|---|---|---|---|---|---|---|---|---|
| tell | rust | default-dispatcher | 100000 | 17.30ms | 5.78M/s | n/a | n/a | n/a | +1.56MiB | 30.00ms |
| tell | python | python-pinned | 20000 | 805.46ms | 24.83k/s | n/a | n/a | n/a | +6.98MiB | 940.00ms |
| ask | rust | default-dispatcher | 5000 | 54.22ms | 92.21k/s | 9.76µs | 15.66µs | 19.89µs | +0B | 80.00ms |
| ask | python | python-pinned | 2000 | 143.49ms | 13.94k/s | 47.15µs | 260.85µs | 379.73µs | +24.00KiB | 150.00ms |
| fanout | rust | default-dispatcher | 2000 | 11.68ms | 171.19k/s | n/a | n/a | n/a | +16.19MiB | 20.00ms |
| fanout | python | python-pinned | 500 | 83.71ms | 5.97k/s | n/a | n/a | n/a | +4.26MiB | 130.00ms |
| cpu | rust | cpu-bound-handler | 10000 | 53.94ms | 185.37k/s | n/a | n/a | n/a | +240.00KiB | 110.00ms |
| cpu | python | python-subinterpreter-pool pool=8 | 2000 | 1.29s | 1.55k/s | n/a | n/a | n/a | +84.00KiB | 1.20s |

## Python overhead factor (python-throughput / rust-throughput)

| scenario | rust | python | python/rust |
|---|---|---|---|
| tell | 5.78M/s | 24.83k/s | 0.43% |
| ask | 92.21k/s | 13.94k/s | 15.12% |
| fanout | 171.19k/s | 5.97k/s | 3.49% |
| cpu | 185.37k/s | 1.55k/s | 0.84% |
