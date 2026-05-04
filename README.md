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
    BatchSize = 5000,          // rows per batch (default: 1000)
    Timeout = TimeSpan.FromMinutes(10),
});
```

Identity behavior is auto-detected per source: if the target's identity column appears in the source's fields, those values are preserved; otherwise the service generates the next sequential values from the table's current identity seed and assigns them to the inserted rows.

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

## License

This package is licensed under the [MIT License](./LICENSE.txt)

Copyright &copy; 2025 triaxis s.r.o.
