using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using triaxis.Data.SqlImportExport;

namespace Bench;

// Through the library, not around it: the order hint is only asserted when the library
// synthesizes the identity itself, so a source that supplies the identity column is the
// control case that gets no hint.
internal static class HintAb
{
    public static async Task RunAsync(string connectionString, int rows, int columns)
    {
        // the big table, and the reported pathology: a 103-column table holding a single row
        foreach (var (table, tableRows, tableColumns) in new[] { ("WideTable", rows, columns), ("Small0", 1, 103) })
        {
            Console.WriteLine($"{table} - {tableRows} row(s) x {tableColumns} columns:");
            foreach (var suppliesIdentity in new[] { true, false })
            {
                await using var con = new SqlConnection(connectionString);
                await con.OpenAsync();
                await Exec(con, $"TRUNCATE TABLE {table};");

                var service = new BulkImportService(NullLogger<BulkImportService>.Instance);
                var monitor = await GrantMonitor.StartAsync(connectionString);
                var sw = Stopwatch.StartNew();
                await service.BulkImportAsync(con, One(new IdentitySource(table, tableColumns, tableRows, suppliesIdentity)),
                    new BulkImportOptions { SkipConstraints = true });
                sw.Stop();
                await monitor.DisposeAsync();

                Console.WriteLine($"  source {(suppliesIdentity ? "supplies" : "omits   ")} identity (hint {(suppliesIdentity ? "off" : "on ")}): " +
                    $"{sw.ElapsedMilliseconds,5} ms, requested {monitor.MaxRequestedKb / 1024.0,6:F1} MB, required {monitor.MaxRequiredKb / 1024.0:F2} MB");
            }
        }
    }

    static async IAsyncEnumerable<IBulkImportSource> One(IBulkImportSource source)
    {
        await Task.CompletedTask;
        yield return source;
    }

    static async Task Exec(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync();
    }

    sealed class IdentitySource(string name, int columns, int rows, bool suppliesIdentity) : IBulkImportSource
    {
        public string Name => name;

        public Task<IEnumerable<string>> GetColumnNamesAsync()
            => Task.FromResult(suppliesIdentity
                ? new[] { "Id" }.Concat(Enumerable.Range(0, columns).Select(i => $"C{i}"))
                : Enumerable.Range(0, columns).Select(i => $"C{i}"));

        public async IAsyncEnumerable<object?[]> EnumerateDataAsync()
        {
            await Task.CompletedTask;
            var epoch = new DateTime(2020, 1, 1);
            int offset = suppliesIdentity ? 1 : 0;
            for (int r = 0; r < rows; r++)
            {
                var row = new object[columns + offset];
                if (suppliesIdentity)
                {
                    row[0] = r + 1;
                }
                for (int c = 0; c < columns; c++)
                {
                    row[c + offset] = (c % 5) switch
                    {
                        0 => r * 31 + c,
                        1 => $"value-{r}-{c}",
                        2 => (r + c) % 2 == 0,
                        3 => epoch.AddMinutes(r % 100000),
                        _ => decimal.Divide((r % 1000) * 100 + c, 7),
                    };
                }
                yield return row;
            }
        }
    }
}
