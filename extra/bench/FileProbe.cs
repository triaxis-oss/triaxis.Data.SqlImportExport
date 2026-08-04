using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;

namespace Bench;

// The claim under test: the oversized grant is caused by the server having no idea how many rows
// are coming. BULK INSERT is the one path that accepts ROWS_PER_BATCH, and OPENROWSET(BULK ...)
// is a real query so it also accepts a grant cap. If cardinality is really the cause, telling the
// server the row count should shrink the request.
internal static class FileProbe
{
    static readonly string[] Pool = Enumerable.Range(0, 1000).Select(i => $"value-{i}").ToArray();
    static readonly DateTime Epoch = new(2020, 1, 1);

    public static async Task RunAsync(string connectionString, int rows, int columns, string localCsv, string serverCsv, bool measure)
    {
        if (!measure)
        {
            WriteCsv(localCsv, rows, columns);
            Console.WriteLine($"wrote {new FileInfo(localCsv).Length / 1024 / 1024} MB CSV ({rows} rows x {columns} columns) to {localCsv}");
            Console.WriteLine("copy it somewhere the server can read, then re-run with: file run");
            return;
        }

        await using var con = new SqlConnection(connectionString);
        await con.OpenAsync();

        var colDefs = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}] {Program.ColumnType(i)} NOT NULL"));
        await Exec(con, "IF OBJECT_ID('FileProbe') IS NOT NULL DROP TABLE FileProbe;");
        await Exec(con, $"CREATE TABLE FileProbe ({colDefs}, CONSTRAINT PK_FileProbe PRIMARY KEY CLUSTERED ([C0]));");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_FileProbe_0 ON FileProbe ([C1], [C2]);");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_FileProbe_1 ON FileProbe ([C3], [C4]);");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_FileProbe_2 ON FileProbe ([C5], [C6]);");

        string common = $"FORMAT = 'CSV', FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a'";

        foreach (var (label, sql) in new[]
        {
            ("BULK INSERT, no row count      ",
                $"BULK INSERT FileProbe FROM '{serverCsv}' WITH ({common});"),
            ("BULK INSERT, ROWS_PER_BATCH    ",
                $"BULK INSERT FileProbe FROM '{serverCsv}' WITH ({common}, ROWS_PER_BATCH = {rows});"),
            ("BULK INSERT, ROWS_PER_BATCH+KB ",
                $"BULK INSERT FileProbe FROM '{serverCsv}' WITH ({common}, ROWS_PER_BATCH = {rows}, KILOBYTES_PER_BATCH = {new FileInfo(localCsv).Length / 1024});"),
            // staging shape: load a heap, then move the rows across with a real query
            ("staged move, no cap           ",
                $"""
                SELECT TOP 0 * INTO #stg FROM FileProbe;
                BULK INSERT #stg FROM '{serverCsv}' WITH ({common});
                INSERT INTO FileProbe SELECT * FROM #stg;
                DROP TABLE #stg;
                """),
            ("staged move, MAX_GRANT_PERCENT",
                $"""
                SELECT TOP 0 * INTO #stg FROM FileProbe;
                BULK INSERT #stg FROM '{serverCsv}' WITH ({common});
                INSERT INTO FileProbe SELECT * FROM #stg OPTION (MAX_GRANT_PERCENT = 2);
                DROP TABLE #stg;
                """),
        })
        {
            await Exec(con, "TRUNCATE TABLE FileProbe;");
            var monitor = await GrantMonitor.StartAsync(connectionString);
            var sw = Stopwatch.StartNew();
            string outcome = "";
            try
            {
                await Exec(con, sql);
            }
            catch (SqlException ex)
            {
                outcome = $" [{ex.Message.Split('\n')[0].Trim()}]";
            }
            sw.Stop();
            await monitor.DisposeAsync();
            Console.WriteLine($"{label}: {sw.ElapsedMilliseconds,5} ms, grant requested {monitor.MaxRequestedKb / 1024.0,6:F1} MB " +
                $"(required {monitor.MaxRequiredKb / 1024.0:F2} MB){outcome}");
        }

        await Exec(con, "IF OBJECT_ID('FileProbe') IS NOT NULL DROP TABLE FileProbe;");
    }

    static void WriteCsv(string path, int rows, int columns)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        w.NewLine = "\n";
        var sb = new StringBuilder();
        for (int r = 0; r < rows; r++)
        {
            sb.Clear();
            for (int c = 0; c < columns; c++)
            {
                if (c > 0)
                {
                    sb.Append(',');
                }
                switch (c % 5)
                {
                    case 0: sb.Append((r * 31 + c).ToString(CultureInfo.InvariantCulture)); break;
                    case 1: sb.Append(Pool[(r + c) % 1000]); break;
                    case 2: sb.Append((r + c) % 2 == 0 ? '1' : '0'); break;
                    case 3: sb.Append(Epoch.AddMinutes(r % 100000).ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)); break;
                    default: sb.Append(decimal.Divide((r % 1000) * 100 + c, 7).ToString("F4", CultureInfo.InvariantCulture)); break;
                }
            }
            w.WriteLine(sb.ToString());
        }
    }

    static async Task Exec(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 600;
        await cmd.ExecuteNonQueryAsync();
    }
}
