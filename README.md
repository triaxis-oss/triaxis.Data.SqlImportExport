# triaxis.Data.SqlImportExport

Bulk import and export helpers for Microsoft SQL Server. Stream entire databases to and from CSV files, or plug in your own data source.

## Installation

```
dotnet add package triaxis.Data.SqlImportExport
```

## Setup

Register the services with the DI container:

```csharp
services.AddSqlImportExport();
```

Then inject `IBulkImportService` and/or `IBulkExportService` where needed.

## Exporting

Export every table in the database to a directory of CSV files (one file per table, named `<TableName>.csv`):

```csharp
await using var con = new SqlConnection(connectionString);
await _export.BulkExportAsync(con).ToCsvDirectoryAsync("output/");
```

### Export options

```csharp
await _export.BulkExportAsync(con, new BulkExportOptions
{
    TableFilter = name => name.StartsWith("Foo"),                  // include only matching tables
    ColumnFilter = (table, column) => column != "PasswordHash",    // exclude specific columns
}).ToCsvDirectoryAsync("output/");
```

## Importing

Import a directory of CSV files into the database. Each file is matched to a table by name:

```csharp
await using var con = new SqlConnection(connectionString);
await _import.BulkImportAsync(con, CsvSource.FromDirectory("output/"));
```

### Import strategies

The default strategy is `Insert`, which uses bulk copy for maximum throughput and fails on duplicate keys. Other strategies use a staging table and a `MERGE` statement:

| Strategy | Behavior |
|---|---|
| `Insert` | Insert all rows; fail on duplicates |
| `Upsert` | Insert new rows; update existing rows by key |
| `InsertIgnore` | Insert new rows; skip existing rows |
| `Truncate` | Truncate the target table, then insert |

Set a strategy globally or per source:

```csharp
// globally
await _import.BulkImportAsync(con, CsvSource.FromDirectory("output/"), new BulkImportOptions
{
    Strategy = BulkImportStrategy.Upsert,
});

// per source (IBulkImportSource.Strategy overrides the global one)
```

### Import options

```csharp
await _import.BulkImportAsync(con, source, new BulkImportOptions
{
    Strategy = BulkImportStrategy.InsertIgnore,
    SkipConstraints = false,   // verify foreign key constraints after import (default: true)
    DryRun = true,             // roll back the transaction at the end; useful for validation
    BatchSize = 0,             // rows per bulk copy batch; 0 sends a single batch (default: 1000)
    MaxBufferedRows = 0,       // memory allowed for regrouping jagged rows (default: 8192; see below)
    Timeout = TimeSpan.FromMinutes(10),
});
```

`BatchSize` is round-trip bound, not memory bound. Streaming is enabled, so rows are not buffered on the client (beyond the bounded `MaxBufferedRows` used for [jagged row shapes](#rows-of-differing-shapes)), and the whole import runs in one transaction, so splitting it buys no durability — every batch just costs another round trip. Loading 164k rows measured 2,208 ms at the default of 1000, 1,157 ms at 10,000, and fastest as a single batch, so raise it or set it to zero for a large import over a connection you trust. It makes no difference to tables holding a handful of rows, which are one batch either way.

Identity behavior is auto-detected per source: if the target's identity column appears in the source's fields, those values are preserved; otherwise the service generates the next sequential values from the table's current identity seed and assigns them to the inserted rows.

### Indexes and memory grants

A bulk insert into an indexed table compiles one Sort per index, and the memory grant is sized to sort the whole input. Requests of tens of megabytes against a stated requirement of half a megabyte are routine and do not scale with the number of rows; on a memory-constrained server they can queue on `RESOURCE_SEMAPHORE`.

It is tempting to read that as the server guessing badly for want of a row count, but the opposite holds: better information makes it ask for *more*, not less. Loading 164k rows measured a request of 17.8 MB against a stated requirement of 0.5 MB; telling the server the exact row count via `BULK INSERT ... ROWS_PER_BATCH` took the request to 283 MB, and every route that plans against a real row count — a staging table, a table-valued parameter, the `MERGE` behind `Upsert` — lands in the same place. What shrinks the request is removing the sorts, or capping it outright.

The one hint `SqlBulkCopy` will carry is a sort order, which spares the server the clustered index sort. A source can [declare its own](#declaring-the-order-rows-arrive-in); failing that, the service asserts it where it generated the identity values itself. Beyond that, `IndexStrategy` chooses what happens to the remaining indexes:

| Strategy | Behavior |
|---|---|
| `Maintain` | Leave indexes in place and let the bulk insert maintain them (default) |
| `Rebuild` | Disable secondary indexes before loading a table, rebuild them at the end |

```csharp
await _import.BulkImportAsync(con, source, new BulkImportOptions
{
    IndexStrategy = BulkImportIndexStrategy.Rebuild,
});
```

Against a 514k-row, 266-table import on SQL Server 2022, `Rebuild` measured 11.7 s to `Maintain`'s 18.7 s, and 15.7 s to 25.8 s once the server's memory was capped at 768 MB. Indexes backing a key, and those a foreign key points at, are never disabled. The disable and the rebuild both run inside the import transaction, so a failure or a `DryRun` leaves every index as it was.

`MaxGrantPercent` caps what the `MERGE` behind `Upsert` and `InsertIgnore` may ask for — the one statement here that plans against a real row count, and so the one whose request scales with the data. Upserting 164k rows asked for 539 MB against a stated requirement of 9 MB; at `MaxGrantPercent = 2` it asked for 41 MB and ran 12% slower, the sort having spilled to tempdb. That is the trade: a slower sort instead of a request the server may not be able to satisfy.

```csharp
await _import.BulkImportAsync(con, source, new BulkImportOptions
{
    Strategy = BulkImportStrategy.Upsert,
    MaxGrantPercent = 2,
});
```

### Inserted ID range

When the source omits the identity column for an `Insert` or `Truncate` import, the return value contains the range of identity values assigned to each table:

```csharp
var ranges = await _import.BulkImportAsync(con, CsvSource.FromDirectory("output/"));

foreach (var range in ranges)
{
    Console.WriteLine($"{range.SourceName}: IDs {range.First}–{range.Last}");
}

// build a lookup if needed
var byTable = ranges.ToDictionary(r => r.SourceName);
```

## Custom import sources

Implement `IBulkImportSource` to supply data from any source:

```csharp
public class MySource : IBulkImportSource
{
    public string Name => "MyTable";
    public BulkImportStrategy? Strategy => null; // use global strategy

    public Task<IEnumerable<string>> GetColumnNamesAsync() =>
        Task.FromResult<IEnumerable<string>>(["Id", "Name", "Value"]);

    public async IAsyncEnumerable<object?[]> EnumerateDataAsync()
    {
        await foreach (var item in GetItemsAsync())
            yield return [item.Id, item.Name, item.Value];
    }
}
```

Then pass it to `BulkImportAsync` as an `IAsyncEnumerable<IBulkImportSource>`.

### Rows of differing shapes

One source can carry rows that don't all supply the same columns. Declare the union of the columns and leave the cells a row doesn't carry as `null`:

```csharp
public Task<IEnumerable<string>> GetColumnNamesAsync() =>
    Task.FromResult<IEnumerable<string>>(["Id", "Name", "Value"]);

public async IAsyncEnumerable<object?[]> EnumerateDataAsync()
{
    yield return [1, "named", 42];
    yield return [2, null, 7];           // no Name: behaves as if the column were omitted
    yield return [3, DBNull.Value, 9];   // explicit NULL, even where the column has a default
}
```

A `null` cell behaves exactly like a statement that omits the column — on insert the default applies when there is one (NULL otherwise), and on an `Upsert` update of a matched row the column is left untouched — while `DBNull.Value` lands as a literal NULL wherever NULL is storable (aimed at a NOT NULL column with a default, where it cannot be, it degrades to omitted — which also keeps CSV files importable into such columns). Both work per cell, with no option to set.

Differently shaped rows mostly share one bulk copy regardless: without `KeepNulls`, a NULL on the wire already means "column default where one exists, NULL otherwise" — exactly what an omitted cell asks for — so omitted cells simply travel as NULLs. A row needs a separate write only when it puts an explicit `DBNull.Value` into a column that has a default (forcing `KeepNulls` for its batch), or omits a NOT NULL column (which must leave the column mapping).

`MaxBufferedRows` (default 8192) decides how much memory may be spent grouping such rows. A source that fits the buffer — seed data, typically — costs one bulk copy per distinct group no matter how its rows interleave. A uniform stretch that outgrows the buffer — the big dataset — streams straight through, never holding more than the buffered rows; only a big source that keeps alternating between groups pays a flush per buffered chunk. Set it to zero to never buffer anything, writing each uniform run as it arrives, or raise it to regroup bigger jagged sources. There is no need to split a table into one source per distinct column set.

Measured against SQL Server 2022 (the `shape` probe in `extra/bench`): 200k jagged rows load in the same time as 200k uniform ones — the grouping never fires when it isn't needed. Rows alternating groups every single row are what the buffer exists for: 20k of them measured 219 ms buffered against 69.6 s written run by run, and a 200-table seed tail of such rows 1.6 s against 41 s. Alternation in 1000-row blocks was indifferent to the setting.

### Declaring the order rows arrive in

A bulk insert into an indexed table otherwise compiles a Sort for the clustered index and asks for a memory grant to run it. A source that already produces rows in clustered key order can say so and skip both:

```csharp
public IEnumerable<SqlBulkCopyColumnOrderHint> SortedBy =>
    [new SqlBulkCopyColumnOrderHint("Id", SortOrder.Ascending)];
```

Loading 164k rows into a table whose only index was the clustered one measured a 17.8 MB grant request without this and none at all with it. Declaring an order the rows are not actually in fails the import, so only say so when the source guarantees it. When left empty and the service is generating the identity values itself, it works the order out on its own — it picked those values, so it knows they ascend.

## License

This package is licensed under the [MIT License](./LICENSE.txt)

Copyright &copy; 2025 triaxis s.r.o.
