using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;

namespace Bench;

// Two questions: is a TVP actually faster than SqlBulkCopy once you repeat the measurement,
// and can many tables be loaded in one round trip by passing one TVP per table to a single batch?
internal static class BatchProbe
{
    static readonly string[] Pool = Enumerable.Range(0, 1000).Select(i => $"value-{i}").ToArray();
    static readonly DateTime Epoch = new(2020, 1, 1);

    public static async Task RunAsync(string connectionString, int wideRows, int wideColumns, int tailTables)
    {
        await using var con = new SqlConnection(connectionString);
        await con.OpenAsync();

        await BigTableAsync(con, connectionString, wideRows, wideColumns);
        await TailAsync(con, connectionString, tailTables);
    }

    // ---- is the TVP really faster on a big table? ----

    static async Task BigTableAsync(SqlConnection con, string connectionString, int rows, int columns)
    {
        var colDefs = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}] {Program.ColumnType(i)} NULL"));
        var colList = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}]"));

        await Exec(con, "IF OBJECT_ID('BigProbe') IS NOT NULL DROP TABLE BigProbe;");
        await Exec(con, "IF TYPE_ID('BigProbeType') IS NOT NULL DROP TYPE BigProbeType;");
        await Exec(con, $"CREATE TABLE BigProbe (Id int IDENTITY(1,1) NOT NULL CONSTRAINT PK_BigProbe PRIMARY KEY CLUSTERED, {colDefs});");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_BigProbe_0 ON BigProbe ([C0], [C1]);");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_BigProbe_1 ON BigProbe ([C2], [C3]);");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_BigProbe_2 ON BigProbe ([C4], [C5]);");
        await Exec(con, $"CREATE TYPE BigProbeType AS TABLE ({colDefs});");

        Console.WriteLine($"== {rows} rows x {columns} columns, 3 iterations each ==");

        var bcpTimes = new List<long>();
        var tvpTimes = new List<long>();

        for (int i = 0; i < 3; i++)
        {
            await Exec(con, "TRUNCATE TABLE BigProbe;");
            var sw = Stopwatch.StartNew();
            using (var bcp = new SqlBulkCopy(con) { DestinationTableName = "BigProbe", EnableStreaming = true, BatchSize = 0, BulkCopyTimeout = 600 })
            {
                for (int c = 0; c < columns; c++)
                {
                    bcp.ColumnMappings.Add(c, $"C{c}");
                }
                await bcp.WriteToServerAsync(new FeedProbeReader(rows, columns));
            }
            bcpTimes.Add(sw.ElapsedMilliseconds);

            await Exec(con, "TRUNCATE TABLE BigProbe;");
            sw.Restart();
            await using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"INSERT INTO BigProbe ({colList}) SELECT {colList} FROM @rows;";
                cmd.CommandTimeout = 600;
                var p = cmd.Parameters.AddWithValue("@rows", TvpRows(rows, columns, 0));
                p.SqlDbType = SqlDbType.Structured;
                p.TypeName = "BigProbeType";
                await cmd.ExecuteNonQueryAsync();
            }
            tvpTimes.Add(sw.ElapsedMilliseconds);
        }

        Console.WriteLine($"  SqlBulkCopy: {string.Join(", ", bcpTimes)} ms  (median {Median(bcpTimes)})");
        Console.WriteLine($"  TVP        : {string.Join(", ", tvpTimes)} ms  (median {Median(tvpTimes)})\n");

        await Exec(con, "DROP TABLE BigProbe;");
        await Exec(con, "DROP TYPE BigProbeType;");
    }

    // ---- can many tables go in one round trip? ----

    static async Task TailAsync(SqlConnection con, string connectionString, int tables)
    {
        // the tail has two column shapes, so two table types cover all of it —
        // types are needed per shape, not per table
        foreach (var cols in new[] { 6, 103 })
        {
            var defs = string.Join(", ", Enumerable.Range(0, cols).Select(i => $"[C{i}] {Program.ColumnType(i)} NULL"));
            await Exec(con, $"IF TYPE_ID('TailType{cols}') IS NOT NULL DROP TYPE TailType{cols};");
            await Exec(con, $"CREATE TYPE TailType{cols} AS TABLE ({defs});");
        }

        Console.WriteLine($"== {tables} near-empty tables (1-9 rows each) ==");

        await Truncate(con, tables);
        var sw = Stopwatch.StartNew();
        for (int t = 0; t < tables; t++)
        {
            int cols = t % 12 == 0 ? 103 : 6;
            using var bcp = new SqlBulkCopy(con) { DestinationTableName = $"Small{t}", EnableStreaming = true, BatchSize = 0, BulkCopyTimeout = 600 };
            for (int c = 0; c < cols; c++)
            {
                bcp.ColumnMappings.Add(c, $"C{c}");
            }
            await bcp.WriteToServerAsync(new FeedProbeReader(1 + (t % 9), cols));
        }
        Console.WriteLine($"  one SqlBulkCopy per table      : {sw.ElapsedMilliseconds,5} ms  ({tables} round trips)");

        foreach (int chunk in new[] { 32, 264 })
        {
            await Truncate(con, tables);
            sw.Restart();
            int roundTrips = 0;
            for (int start = 0; start < tables; start += chunk)
            {
                int end = Math.Min(start + chunk, tables);
                await using var cmd = con.CreateCommand();
                cmd.CommandTimeout = 600;
                var sql = new System.Text.StringBuilder();
                for (int t = start; t < end; t++)
                {
                    int cols = t % 12 == 0 ? 103 : 6;
                    var colList = string.Join(", ", Enumerable.Range(0, cols).Select(i => $"[C{i}]"));
                    sql.AppendLine($"INSERT INTO Small{t} ({colList}) SELECT {colList} FROM @p{t};");
                    var p = cmd.Parameters.AddWithValue($"@p{t}", TvpRows(1 + (t % 9), cols, t));
                    p.SqlDbType = SqlDbType.Structured;
                    p.TypeName = $"TailType{cols}";
                }
                cmd.CommandText = sql.ToString();
                await cmd.ExecuteNonQueryAsync();
                roundTrips++;
            }
            Console.WriteLine($"  {chunk,3} tables per batch (TVPs)    : {sw.ElapsedMilliseconds,5} ms  ({roundTrips} round trip{(roundTrips == 1 ? "" : "s")})");
        }

        foreach (int cols in new[] { 6, 103 })
        {
            await Exec(con, $"DROP TYPE TailType{cols};");
        }
    }

    static async Task Truncate(SqlConnection con, int tables)
    {
        for (int start = 0; start < tables; start += 50)
        {
            var sql = string.Join(" ", Enumerable.Range(start, Math.Min(50, tables - start)).Select(t => $"TRUNCATE TABLE Small{t};"));
            await Exec(con, sql);
        }
    }

    static long Median(List<long> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }

    static SqlMetaData Meta(int i) => (i % 5) switch
    {
        0 => new SqlMetaData($"C{i}", SqlDbType.Int),
        1 => new SqlMetaData($"C{i}", SqlDbType.NVarChar, 100),
        2 => new SqlMetaData($"C{i}", SqlDbType.Bit),
        3 => new SqlMetaData($"C{i}", SqlDbType.DateTime2),
        _ => new SqlMetaData($"C{i}", SqlDbType.Decimal, 18, 4),
    };

    static IEnumerable<SqlDataRecord> TvpRows(int rows, int columns, int salt)
    {
        var record = new SqlDataRecord(Enumerable.Range(0, columns).Select(Meta).ToArray());
        for (int r = 0; r < rows; r++)
        {
            for (int c = 0; c < columns; c++)
            {
                switch (c % 5)
                {
                    case 0: record.SetInt32(c, (r + salt) * 31 + c); break;
                    case 1: record.SetString(c, Pool[(r + c) % 1000]); break;
                    case 2: record.SetBoolean(c, (r + c) % 2 == 0); break;
                    case 3: record.SetDateTime(c, Epoch.AddMinutes(r % 100000)); break;
                    default: record.SetDecimal(c, decimal.Divide((r % 1000) * 100 + c, 7)); break;
                }
            }
            yield return record;
        }
    }

    static async Task Exec(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 600;
        await cmd.ExecuteNonQueryAsync();
    }

    sealed class FeedProbeReader : IDataReader
    {
        private readonly int _rows;
        private readonly int _columns;
        private object[] _row = null!;
        private int _index = -1;

        public FeedProbeReader(int rows, int columns) { _rows = rows; _columns = columns; }

        public bool Read()
        {
            if (++_index >= _rows)
            {
                return false;
            }
            _row = new object[_columns];
            for (int c = 0; c < _columns; c++)
            {
                _row[c] = (c % 5) switch
                {
                    0 => _index * 31 + c,
                    1 => Pool[(_index + c) % 1000],
                    2 => (_index + c) % 2 == 0,
                    3 => Epoch.AddMinutes(_index % 100000),
                    _ => decimal.Divide((_index % 1000) * 100 + c, 7),
                };
            }
            return true;
        }

        public object GetValue(int i) => _row[i];
        public int FieldCount => _columns;
        public string GetName(int i) => $"C{i}";
        public int GetOrdinal(string name) => int.Parse(name.Substring(1));
        public Type GetFieldType(int i) => _row[i].GetType();
        public bool IsDBNull(int i) => false;
        public int GetValues(object[] values) { _row.CopyTo(values, 0); return _columns; }
        public object this[int i] => _row[i];
        public object this[string name] => _row[GetOrdinal(name)];
        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => 0;
        public void Close() { }
        public void Dispose() { }
        public bool NextResult() => false;
        public bool GetBoolean(int i) => (bool)_row[i];
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long o, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long o, char[]? b, int bo, int l) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public DateTime GetDateTime(int i) => (DateTime)_row[i];
        public decimal GetDecimal(int i) => (decimal)_row[i];
        public double GetDouble(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public int GetInt32(int i) => (int)_row[i];
        public long GetInt64(int i) => throw new NotSupportedException();
        public string GetString(int i) => (string)_row[i];
        public DataTable? GetSchemaTable() => null;
    }
}
