# extra

Things that are not part of the package. Nothing here is built by CI — the solution in `src`
does not reference it — so it will rot unless someone runs it.

## bench

The harness the bulk import performance work was measured with, plus the probes that decided it.
It is kept because most of those probes disproved something, and the next person to have the same
plausible idea should be able to re-run the measurement instead of re-deriving the disappointment.

### Running it

Needs a SQL Server. The connection string defaults to the one the integration tests use:

```
docker run -d --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='YourStrong@Passw0rd' \
    -e MSSQL_PID=Developer -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

cd extra/bench
dotnet run -c Release                    # full import, 3 iterations, default options
dotnet run -c Release -- run 3 0         # ... with BatchSize 0
dotnet run -c Release -- run 3 - Rebuild # ... with IndexStrategy.Rebuild
```

Arguments are `<label> <iterations> <batchSize|-> <indexStrategy|->`. It drops and recreates a
`BulkImportBench` database on every iteration.

Environment variables:

| variable | effect |
|---|---|
| `BENCH_WATCH_GRANTS=1` | sample `sys.dm_exec_query_memory_grants` and `RESOURCE_SEMAPHORE` waits during the run |
| `BENCH_TAIL_ONLY=1` | skip the two big tables, load only the 264 near-empty ones |
| `BENCH_FREE_CACHE=1` | `DBCC FREEPROCCACHE` before each iteration |
| `BENCH_MAX_GRANT_PERCENT=n` | pass `MaxGrantPercent` |

### The workload

Modelled on a production export loaded by `prepare-test`: one 164k-row × 51-column table, one
350k-row × 8-column table, and 264 near-empty tables (1–9 rows, every twelfth one 103 columns
wide), all with a clustered identity PK and two or three nonclustered indexes. ~514k rows over
266 tables.

### Probes

Each is a mode of the same binary: `dotnet run -c Release -- <mode>`.

| mode | question | answer |
|---|---|---|
| `probe` | what does the per-table metadata query cost? | interpolated 29 ms/table, parameterised 1.3 ms, one batched query 0.2 ms — this was the single biggest win |
| `typed` | does a typed `IDataReader` avoid boxing? | **no.** `SqlBulkCopy` called `GetValue` 8,364,000 times and the typed accessors zero times |
| `orderhint` | does the ORDER hint shrink the grant? | clustered index only: 17.8 MB → **0**. With wide secondary indexes still sorting: unchanged, but 18% faster |
| `hintab` | does the hint work through the library? | yes, and it only fires where the library generates the identity values |
| `feed` | is a table-valued parameter better than bulk copy? | same speed (3622 vs 3575 ms), **half the allocation** (298 → 137 MB), **ten times the grant** (4 → 40 MB) |
| `batch` | can many tables load in one round trip? | yes — 264 tables, 2002 ms in 264 round trips vs **1018 ms in one**. Types are needed per column shape, not per table |
| `file` | does telling the server the row count help? | **no, it hurts.** `BULK INSERT ... ROWS_PER_BATCH` took the request from 17.8 MB to 283 MB |
| `mergegrant` | does `MaxGrantPercent` work on the MERGE path? | 539 MB → 41 MB requested, 12% slower as the sort spills |
| `alloc` | how much of the allocation is the caller's? | 447 MB of 557 MB, before the library sees a row |

`file` needs the CSV somewhere the server can read it, so it runs in two steps:

```
dotnet run -c Release -- file        # writes ./feed.csv
docker cp feed.csv mssql:/var/opt/mssql/data/feed.csv
docker exec -u root mssql chown mssql /var/opt/mssql/data/feed.csv
dotnet run -c Release -- file run    # measures
```

### What this established

The memory grant is not oversized because the server lacks a row count. Every route that gives it
a real one asks for **more**: `ROWS_PER_BATCH` 283 MB, a temp-heap staging insert 284 MB, a
table-valued parameter 40 MB, the `Upsert` MERGE 539 MB — against a blind bulk insert's 17.8 MB and
a stated requirement of 0.5 MB. What shrinks it is removing the sorts (`SortedBy`, and
`IndexStrategy.Rebuild`) or capping the request (`MaxGrantPercent`).

Two things were built, measured, and dropped rather than shipped: a temp-heap `Stage` strategy
(slower everywhere, and it timed out on a 768 MB server), and a typed source API for item 5 of the
original brief (`SqlBulkCopy` never calls the typed accessors, so it cannot pay off).

Comparing against an older build means building that revision in a worktree and copying its
assembly over `bin/Release/net8.0/triaxis.Data.SqlImportExport.dll`. The harness prints
`DefaultBatchSize` and the assembly timestamp on startup for exactly this reason — an earlier
round of "before" numbers in this work turned out to be the new code benchmarked against itself.
