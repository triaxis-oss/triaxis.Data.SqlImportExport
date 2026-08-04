using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using triaxis.Data.SqlImportExport;

namespace Bench;

// MaxGrantPercent now only reaches the MERGE behind Upsert/InsertIgnore, so the claim made for it
// has to be measured on that path rather than on the staged insert it was first measured on.
internal static class MergeGrantProbe
{
    public static async Task RunAsync(string connectionString, int rows, int columns)
    {
        foreach (int? cap in new int?[] { null, 2 })
        {
            await using var con = new SqlConnection(connectionString);
            await con.OpenAsync();
            await Exec(con, "TRUNCATE TABLE WideTable;");

            var service = new BulkImportService(NullLogger<BulkImportService>.Instance);
            var options = new BulkImportOptions
            {
                Strategy = BulkImportStrategy.Upsert,
                MaxGrantPercent = cap,
                SkipConstraints = true,
            };

            var monitor = await GrantMonitor.StartAsync(connectionString);
            var sw = Stopwatch.StartNew();
            await service.BulkImportAsync(con, One(new GeneratedSource("WideTable", columns, rows)), options);
            sw.Stop();
            await monitor.DisposeAsync();

            Console.WriteLine($"  Upsert, MaxGrantPercent={(cap?.ToString() ?? "unset"),5}: {sw.ElapsedMilliseconds,6} ms, " +
                $"requested {monitor.MaxRequestedKb / 1024.0,7:F1} MB (required {monitor.MaxRequiredKb / 1024.0:F2} MB)");
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
        cmd.CommandTimeout = 600;
        await cmd.ExecuteNonQueryAsync();
    }
}
