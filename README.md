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
    KeepNulls = true,          // don't substitute column defaults for null values
    SkipConstraints = false,   // verify foreign key constraints after import (default: true)
    DryRun = true,             // roll back the transaction at the end; useful for validation
    BatchSize = 0,             // rows per bulk copy batch; 0 sends a single batch (default: 1000)
    Timeout = TimeSpan.FromMinutes(10),
});
```

`BatchSize` is round-trip bound, not memory bound. Streaming is enabled, so rows are never buffered on the client, and the whole import runs in one transaction, so splitting it buys no durability — every batch just costs another round trip. Loading 164k rows measured 2,208 ms at the default of 1000, 1,157 ms at 10,000, and fastest as a single batch, so raise it or set it to zero for a large import over a connection you trust. It makes no difference to tables holding a handful of rows, which are one batch either way.

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

    public async IAsyncEnumerable<object[]> EnumerateDataAsync()
    {
        await foreach (var item in GetItemsAsync())
            yield return [item.Id, item.Name, item.Value];
    }
}
```

Then pass it to `BulkImportAsync` as an `IAsyncEnumerable<IBulkImportSource>`.

### Rows of differing shapes

One source can carry rows that don't all supply the same columns. Declare the union of the columns and pass `DBNull.Value` for the ones a row doesn't carry:

```csharp
public Task<IEnumerable<string>> GetColumnNamesAsync() =>
    Task.FromResult<IEnumerable<string>>(["Id", "Name", "Value"]);

public async IAsyncEnumerable<object[]> EnumerateDataAsync()
{
    yield return [1, "named", 42];
    yield return [2, DBNull.Value, 7];   // no Name: takes the column default
}
```

Those land in the destination as the column's default, or as NULL when `KeepNulls` is set. There is no need to split a table into one source per distinct column set.

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
