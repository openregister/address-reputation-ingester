# Performance Benchmarks

All these were run on the same developer laptop. The absolute measurements are not meaningful, but their relative
sizes are useful indications. Mongo and Apache2 were also running on the same machine.

## Ingest only abi/42/full (forced)

| heap variant           | heap   | measurements        |
| ---------------------- | ------ | ------------------- |
| chronicle in-memory    | 2048M  | 33.5s, 31.9s, 31.1s |
| concurrent heap memory | 2048M  | 27.9s, 25.1s, 27.4s |

Bulk size = 1000 (default)
Loop delay = 0 (default)

## Ingest only abp/42/full (forced)

| heap variant           | heap   | measurements        |
| ---------------------- | ------ | ------------------- |
| chronicle in-memory    | 2048M  | 1172s               |
| concurrent heap memory | 4096M  | ran out of memory   |

Bulk size = 1000 (default)
Loop delay = 0 (default)
