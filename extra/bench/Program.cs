using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using triaxis.Data.SqlImportExport;

namespace Bench;

// Reproduces the reported prepare-test workload: one wide 164k-row table, one narrower
// bulk table, and a long tail of near-empty tables (some very wide), all indexed the way
// production tables are. Everything is generated deterministically so before/after runs
// are comparable.
internal static class Program
{
    const string Base = "Server=localhost,1433;User Id=sa;Password=YourStrong@Passw0rd;Encrypt=False;TrustServerCertificate=True;";
    const string DbName = "BulkImportBench";

    const int WideRows = 164_000;
    const int WideCols = 51;
    const int MediumRows = 350_000;
    const int SmallTables = 264;
    const int VeryWideCols = 103;

    static string MasterCs => new SqlConnectionStringBuilder(Base) { InitialCatalog = "master" }.ToString();
    static string BenchCs => new SqlConnectionStringBuilder(Base) { InitialCatalog = DbName }.ToString();

    static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "alloc")
        {
            // what the sources allocate on their own, with no library and no server involved:
            // the object[] rows and the boxes inside them that the IBulkImportSource shape requires
            var before = GC.GetTotalAllocatedBytes();
            long rows = 0;
            await foreach (var source in Sources(Stopwatch.StartNew(), new Dictionary<string, double>()))
            {
                await foreach (var row in source.EnumerateDataAsync())
                {
                    rows++;
                }
            }
            Console.WriteLine($"source-side only: {(GC.GetTotalAllocatedBytes() - before) / 1024.0 / 1024.0:F0} MB for {rows} rows");
            return 0;
        }

        if (args.Length > 0 && args[0] == "mergegrant")
        {
            await ResetDatabaseAsync();
            await MergeGrantProbe.RunAsync(BenchCs, WideRows, WideCols);
            return 0;
        }

        if (args.Length > 0 && args[0] == "batch")
        {
            await ResetDatabaseAsync();
            await BatchProbe.RunAsync(BenchCs, WideRows, WideCols, SmallTables);
            return 0;
        }

        if (args.Length > 0 && args[0] == "file")
        {
            // the server reads the file itself, so it has to be somewhere the server can see:
            //   dotnet run -- file            writes ./feed.csv and stops
            //   docker cp feed.csv mssql:/var/opt/mssql/data/feed.csv
            //   docker exec -u root mssql chown mssql /var/opt/mssql/data/feed.csv
            //   dotnet run -- file run        measures against it
            var local = Path.Combine(Directory.GetCurrentDirectory(), "feed.csv");
            var onServer = Environment.GetEnvironmentVariable("BENCH_SERVER_CSV") ?? "/var/opt/mssql/data/feed.csv";
            bool measure = args.Length > 1 && args[1] == "run";
            if (!measure)
            {
                await ResetDatabaseAsync();
            }
            await FileProbe.RunAsync(BenchCs, WideRows, WideCols, local, onServer, measure);
            return 0;
        }

        if (args.Length > 0 && args[0] == "feed")
        {
            await ResetDatabaseAsync();
            await FeedProbe.RunAsync(BenchCs, WideRows, WideCols);
            return 0;
        }

        if (args.Length > 0 && args[0] == "hintab")
        {
            await ResetDatabaseAsync();
            await HintAb.RunAsync(BenchCs, WideRows, WideCols);
            return 0;
        }

        if (args.Length > 0 && args[0] == "orderhint")
        {
            await ResetDatabaseAsync();
            await OrderHintProbe.RunAsync(BenchCs, WideRows, WideCols);
            return 0;
        }

        if (args.Length > 0 && args[0] == "typed")
        {
            await ResetDatabaseAsync();
            await TypedProbe.RunAsync(BenchCs, WideRows, WideCols);
            return 0;
        }

        if (args.Length > 0 && args[0] == "probe")
        {
            await ResetDatabaseAsync();
            await Probe.RunAsync(SmallTables);
            return 0;
        }

        // comparing against an older build means swapping its assembly into this one's output
        // directory, so say which one actually got loaded rather than trusting the checkout
        Console.WriteLine($"library: DefaultBatchSize={BulkImportOptions.DefaultBatchSize}, " +
            $"built {File.GetLastWriteTime(typeof(BulkImportOptions).Assembly.Location):yyyy-MM-dd HH:mm:ss}");

        var label = args.Length > 0 ? args[0] : "run";
        int repeats = args.Length > 1 ? int.Parse(args[1]) : 3;
        var batchSize = args.Length > 2 && args[2] != "-" ? (int?)int.Parse(args[2]) : null;
        var grant = Environment.GetEnvironmentVariable("BENCH_MAX_GRANT_PERCENT");

        var options = new BulkImportOptions
        {
            BatchSize = batchSize,
            IndexStrategy = args.Length > 3 && args[3] != "-"
                ? Enum.Parse<BulkImportIndexStrategy>(args[3], ignoreCase: true)
                : BulkImportIndexStrategy.Maintain,
            MaxGrantPercent = grant != null ? int.Parse(grant) : null,
        };

        bool watchGrants = Environment.GetEnvironmentVariable("BENCH_WATCH_GRANTS") == "1";

        var results = new List<Run>();
        for (int i = 0; i < repeats; i++)
        {
            await ResetDatabaseAsync();
            if (Environment.GetEnvironmentVariable("BENCH_FREE_CACHE") == "1")
            {
                // a fresh server has never seen these query texts; the per-table metadata query
                // interpolates the table name, so a cold cache pays a compile for every table
                await using var flush = new SqlConnection(MasterCs);
                await flush.OpenAsync();
                await Exec(flush, "DBCC FREEPROCCACHE WITH NO_INFOMSGS;");
            }
            var monitor = watchGrants ? await GrantMonitor.StartAsync(BenchCs) : null;
            var run = await ImportOnceAsync(options);
            if (monitor != null)
            {
                await monitor.DisposeAsync();
                Console.WriteLine($"  [{label}] {monitor.Report()}");
            }
            results.Add(run);
            Console.WriteLine($"  [{label}] iteration {i + 1}: total {run.TotalMs:F0} ms  (wide {run.WideMs:F0} ms, medium {run.MediumMs:F0} ms, tail {run.TailMs:F0} ms, alloc {run.AllocMb:F0} MB, GCs {run.Gcs})");
        }

        var best = results.OrderBy(r => r.TotalMs).ToList();
        var median = best[best.Count / 2];
        Console.WriteLine();
        Console.WriteLine($"RESULT {label}: median total {median.TotalMs:F0} ms | wide {median.WideMs:F0} | medium {median.MediumMs:F0} | tail({SmallTables}) {median.TailMs:F0} | per-tail-table {median.TailMs / SmallTables:F1} ms | alloc {median.AllocMb:F0} MB | GCs {median.Gcs}");
        return 0;
    }

    record Run(double TotalMs, double WideMs, double MediumMs, double TailMs, double AllocMb, int Gcs);

    static async Task<Run> ImportOnceAsync(BulkImportOptions options)
    {
        var service = new BulkImportService(NullLogger<BulkImportService>.Instance);
        await using var con = new SqlConnection(BenchCs);
        await con.OpenAsync();

        var timings = new Dictionary<string, double>();
        var sw = Stopwatch.StartNew();
        var allocBefore = GC.GetTotalAllocatedBytes();
        int gcBefore = GC.CollectionCount(0) + GC.CollectionCount(1) + GC.CollectionCount(2);

        var total = Stopwatch.StartNew();
        await service.BulkImportAsync(con, Sources(sw, timings), options);
        total.Stop();

        double alloc = (GC.GetTotalAllocatedBytes() - allocBefore) / 1024.0 / 1024.0;
        int gcs = GC.CollectionCount(0) + GC.CollectionCount(1) + GC.CollectionCount(2) - gcBefore;

        double tail = timings.Where(kv => kv.Key.StartsWith("Small")).Sum(kv => kv.Value);
        return new Run(total.Elapsed.TotalMilliseconds, timings["WideTable"], timings["MediumTable"], tail, alloc, gcs);
    }

    // per-source wall clock is measured by stamping the moment each source is handed over and
    // the moment the next one is requested — the gap is what the library spent on that source
    static async IAsyncEnumerable<IBulkImportSource> Sources(Stopwatch clock, Dictionary<string, double> timings)
    {
        string? pending = null;
        double start = 0;

        void Stamp(string name)
        {
            if (pending != null)
            {
                timings[pending] = clock.Elapsed.TotalMilliseconds - start;
            }
            pending = name;
            start = clock.Elapsed.TotalMilliseconds;
        }

        await Task.CompletedTask;

        bool tailOnly = Environment.GetEnvironmentVariable("BENCH_TAIL_ONLY") == "1";

        Stamp("WideTable");
        if (!tailOnly)
        {
            yield return new GeneratedSource("WideTable", WideCols, WideRows);
        }

        Stamp("MediumTable");
        if (!tailOnly)
        {
            yield return new GeneratedSource("MediumTable", 8, MediumRows);
        }

        for (int t = 0; t < SmallTables; t++)
        {
            Stamp($"Small{t}");
            // every 12th tail table is very wide, like the 103-column one-row table in the report
            int cols = t % 12 == 0 ? VeryWideCols : 6;
            yield return new GeneratedSource($"Small{t}", cols, 1 + (t % 9));
        }

        Stamp("done");
        timings["done"] = 0;
    }

    // ---------- schema ----------

    static async Task ResetDatabaseAsync()
    {
        await using var master = new SqlConnection(MasterCs);
        await master.OpenAsync();
        await Exec(master, $"""
            IF DB_ID('{DbName}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{DbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{DbName}];
            END;
            """);
        await Exec(master, $"CREATE DATABASE [{DbName}];");

        await using var con = new SqlConnection(BenchCs);
        await con.OpenAsync();
        await Exec(con, CreateTableSql("WideTable", WideCols, indexes: 3));
        await Exec(con, CreateTableSql("MediumTable", 8, indexes: 2));

        var batch = new StringBuilder();
        for (int t = 0; t < SmallTables; t++)
        {
            int cols = t % 12 == 0 ? VeryWideCols : 6;
            batch.AppendLine(CreateTableSql($"Small{t}", cols, indexes: 2));
            if (t % 20 == 19)
            {
                await Exec(con, batch.ToString());
                batch.Clear();
            }
        }
        if (batch.Length > 0)
        {
            await Exec(con, batch.ToString());
        }
    }

    // column i's type cycles so every table mixes ints, strings, bits, dates and decimals
    public static string ColumnType(int i) => (i % 5) switch
    {
        0 => "int",
        1 => "nvarchar(100)",
        2 => "bit",
        3 => "datetime2",
        _ => "decimal(18,4)",
    };

    static string CreateTableSql(string name, int columns, int indexes)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE [{name}] (");
        sb.AppendLine("  Id int IDENTITY(1,1) NOT NULL,");
        for (int i = 0; i < columns; i++)
        {
            sb.AppendLine($"  [C{i}] {ColumnType(i)} NULL,");
        }
        sb.AppendLine($"  CONSTRAINT [PK_{name}] PRIMARY KEY CLUSTERED (Id)");
        sb.AppendLine(");");
        for (int x = 0; x < indexes; x++)
        {
            // index leading columns picked so at least one is a string and one an int
            int c = (x * 2) % columns;
            sb.AppendLine($"CREATE NONCLUSTERED INDEX [IX_{name}_{x}] ON [{name}] ([C{c}], [C{(c + 1) % columns}]);");
        }
        return sb.ToString();
    }

    static async Task Exec(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync();
    }
}

// Mimics a columnar Parquet read: typed column arrays materialised into object[] per row,
// which is exactly what the caller does today.
internal sealed class GeneratedSource : IBulkImportSource
{
    private readonly int _columns;
    private readonly int _rows;

    public GeneratedSource(string name, int columns, int rows)
    {
        Name = name;
        _columns = columns;
        _rows = rows;
    }

    public string Name { get; }

    public Task<IEnumerable<string>> GetColumnNamesAsync()
        => Task.FromResult(Enumerable.Range(0, _columns).Select(i => $"C{i}"));

    public async IAsyncEnumerable<object?[]> EnumerateDataAsync()
    {
        await Task.CompletedTask;
        var epoch = new DateTime(2020, 1, 1);
        for (int r = 0; r < _rows; r++)
        {
            var row = new object[_columns];
            for (int c = 0; c < _columns; c++)
            {
                row[c] = (c % 5) switch
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
