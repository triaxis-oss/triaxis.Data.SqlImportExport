using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using triaxis.Data.SqlImportExport;

namespace Bench;

// Measures the jagged-row (write-class) handling across the situations it is designed around:
//
//   uniform             the big consistent dataset - the baseline every other row mix is
//                       measured against; must stream whatever MaxBufferedRows says
//   jagged nullable     omitted cells in nullable columns only - collapses into the same
//                       single write as uniform (omitted rides as DBNull with KeepNulls off),
//                       so it should cost the same
//   NULL-in-default     a literal NULL in a defaulted column forces KeepNulls for its rows -
//                       the one per-cell state that cannot share the plain write; alternating
//                       per row is the worst case, per 1000-row block the realistic one
//   omit NOT NULL       an omitted NOT NULL column must leave the mapping (SqlBulkCopy rejects
//                       the DBNull client-side), the other unavoidable class split
//   seed tail           many small jagged tables in one import - the round-trip-bound case
//                       buffering exists for
//   upsert jagged       the staged merge path, whose cost should not depend on shapes at all
//
// Each situation runs under the MaxBufferedRows settings where the outcome differs: 0 writes
// every uniform run as it arrives (strictly streaming), the default regroups within the buffer,
// unbounded buffers whole sources.
internal static class ShapeProbe
{
    static readonly string[] Cols = ["A", "B", "C"];

    public static async Task RunAsync(string cs)
    {
        await using var con = new SqlConnection(cs);
        await con.OpenAsync();
        var service = new BulkImportService(NullLogger<BulkImportService>.Instance);

        // warm the connection, plan cache and JIT before any timing
        await PrepareTableAsync(con);
        await service.BulkImportAsync(con, One(new ShapeSource("Shape", Cols, 2_000, Uniform)));

        Console.WriteLine($"{"scenario",-28} {"rows",9}  {"MaxBufferedRows",-15} {"ms",9} {"MB",7}");

        foreach (var (name, rows, factory, buffers) in Scenarios())
        {
            foreach (var buffer in buffers)
            {
                await PrepareTableAsync(con);
                var (ms, mb) = await MeasureAsync(service, con,
                    One(new ShapeSource("Shape", Cols, rows, factory)),
                    new BulkImportOptions { MaxBufferedRows = buffer });
                Report(name, rows, buffer, ms, mb);
            }
        }

        await SeedTailAsync(con, service);
        await UpsertJaggedAsync(con, service);
    }

    static IEnumerable<(string Name, int Rows, Func<int, object?[]> Factory, int?[] Buffers)> Scenarios()
    {
        // uniform and jagged-nullable are a single write class either way, so buffering can
        // only add overhead there - 0 vs default shows how much
        yield return ("uniform", 200_000, Uniform, [0, null]);
        yield return ("jagged nullable", 200_000, JaggedNullable, [0, null]);
        // per-row alternation at MaxBufferedRows = 0 degenerates into a bulk copy per row -
        // fewer rows keep the probe finite while the cliff stays plainly visible
        yield return ("NULL-in-default / row", 20_000, NullInDefaultPerRow, [0, null, int.MaxValue]);
        yield return ("NULL-in-default / 1000-block", 200_000, NullInDefaultBlocks, [0, null, int.MaxValue]);
        yield return ("omit NOT NULL / row", 20_000, OmitNotNullPerRow, [0, null, int.MaxValue]);
    }

    static object?[] Uniform(int i) => [$"a{i}", i, $"c{i}"];
    static object?[] JaggedNullable(int i) => [i % 3 == 0 ? null : $"a{i}", i % 2 == 0 ? null : i, $"c{i}"];
    static object?[] NullInDefaultPerRow(int i) => [i % 2 == 1 ? DBNull.Value : $"a{i}", i, $"c{i}"];
    static object?[] NullInDefaultBlocks(int i) => [(i / 1000) % 2 == 1 ? DBNull.Value : $"a{i}", i, $"c{i}"];
    static object?[] OmitNotNullPerRow(int i) => [$"a{i}", i, i % 2 == 1 ? null : $"c{i}"];

    static async Task SeedTailAsync(SqlConnection con, BulkImportService service)
    {
        const int tables = 200, rows = 60;

        var create = new StringBuilder();
        for (int t = 0; t < tables; t++)
        {
            create.AppendLine($"CREATE TABLE Seed{t} (Id int IDENTITY(1,1) PRIMARY KEY, {ColumnsSql});");
        }
        await ExecAsync(con, create.ToString());

        foreach (int? buffer in (int?[])[0, null])
        {
            var reset = new StringBuilder();
            for (int t = 0; t < tables; t++)
            {
                reset.AppendLine($"TRUNCATE TABLE Seed{t};");
            }
            await ExecAsync(con, reset.ToString());

            var (ms, mb) = await MeasureAsync(service, con,
                Many(Enumerable.Range(0, tables).Select(t => new ShapeSource($"Seed{t}", Cols, rows, NullInDefaultPerRow))),
                new BulkImportOptions { MaxBufferedRows = buffer });
            Report($"seed tail ({tables} tables)", tables * rows, buffer, ms, mb);
        }
    }

    static async Task UpsertJaggedAsync(SqlConnection con, BulkImportService service)
    {
        const int rows = 20_000;
        await ExecAsync(con, $"CREATE TABLE UShape (Id int PRIMARY KEY, {ColumnsSql});");

        // half the keys pre-exist so both merge branches do real work
        var seed = new ShapeSource("UShape", ["Id", "A", "B", "C"], rows / 2, i => [i * 2, $"a{i}", i, $"c{i}"]);
        await service.BulkImportAsync(con, One(seed));

        var jagged = new ShapeSource("UShape", ["Id", "A", "B", "C"], rows,
            i => [i, i % 4 == 1 ? DBNull.Value : i % 4 == 2 ? null : $"u{i}", i % 2 == 0 ? null : i, $"c{i}"],
            BulkImportStrategy.Upsert);
        var (ms, mb) = await MeasureAsync(service, con, One(jagged), new BulkImportOptions());
        Report("upsert jagged (merge path)", rows, null, ms, mb);
    }

    const string ColumnsSql = "A nvarchar(32) NULL DEFAULT 'defA', B int NULL, C nvarchar(32) NOT NULL DEFAULT 'defC'";

    static async Task PrepareTableAsync(SqlConnection con)
        => await ExecAsync(con, $"DROP TABLE IF EXISTS Shape; CREATE TABLE Shape (Id int IDENTITY(1,1) PRIMARY KEY, {ColumnsSql});");

    static async Task<(double Ms, double Mb)> MeasureAsync(BulkImportService service, SqlConnection con, IAsyncEnumerable<IBulkImportSource> sources, BulkImportOptions options)
    {
        var allocBefore = GC.GetTotalAllocatedBytes();
        var sw = Stopwatch.StartNew();
        await service.BulkImportAsync(con, sources, options);
        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds, (GC.GetTotalAllocatedBytes() - allocBefore) / 1024.0 / 1024.0);
    }

    static void Report(string name, int rows, int? buffer, double ms, double mb)
        => Console.WriteLine($"{name,-28} {rows,9}  {BufferLabel(buffer),-15} {ms,9:F0} {mb,7:F0}");

    static string BufferLabel(int? buffer) => buffer switch
    {
        0 => "0 (stream)",
        null => "default",
        int.MaxValue => "unbounded",
        _ => buffer.Value.ToString(),
    };

    static async Task ExecAsync(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync();
    }

    static async IAsyncEnumerable<IBulkImportSource> One(IBulkImportSource source)
    {
        await Task.CompletedTask;
        yield return source;
    }

    static async IAsyncEnumerable<IBulkImportSource> Many(IEnumerable<IBulkImportSource> sources)
    {
        await Task.CompletedTask;
        foreach (var s in sources)
        {
            yield return s;
        }
    }

    sealed class ShapeSource(string name, string[] cols, int rows, Func<int, object?[]> factory, BulkImportStrategy? strategy = null) : IBulkImportSource
    {
        public string Name => name;
        public BulkImportStrategy? Strategy => strategy;

        public Task<IEnumerable<string>> GetColumnNamesAsync() => Task.FromResult<IEnumerable<string>>(cols);

        public async IAsyncEnumerable<object?[]> EnumerateDataAsync()
        {
            await Task.CompletedTask;
            for (int i = 0; i < rows; i++)
            {
                yield return factory(i);
            }
        }
    }
}
