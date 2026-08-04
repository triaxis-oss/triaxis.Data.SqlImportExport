using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;

namespace Bench;

// SqlBulkCopy boxes every scalar at GetValue and never tells the server a row count.
// A table-valued parameter does neither: SqlDataRecord has typed setters, one record can be
// reused for every row, and INSERT ... SELECT FROM @tvp is a real query with a real estimate.
// Worth knowing what that actually costs.
internal static class FeedProbe
{
    // string values come from a fixed pool so neither path is charged for building them —
    // the allocation difference that remains is boxing and the object[] wrapper
    static readonly string[] Pool = Enumerable.Range(0, 1000).Select(i => $"value-{i}").ToArray();
    static readonly DateTime Epoch = new(2020, 1, 1);

    public static async Task RunAsync(string connectionString, int rows, int columns)
    {
        await using var con = new SqlConnection(connectionString);
        await con.OpenAsync();

        var colDefs = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}] {Program.ColumnType(i)} NULL"));
        var colList = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}]"));

        await Exec(con, "IF OBJECT_ID('FeedProbe') IS NOT NULL DROP TABLE FeedProbe;");
        await Exec(con, "IF TYPE_ID('FeedProbeType') IS NOT NULL DROP TYPE FeedProbeType;");
        await Exec(con, $"CREATE TABLE FeedProbe (Id int IDENTITY(1,1) NOT NULL CONSTRAINT PK_FeedProbe PRIMARY KEY CLUSTERED, {colDefs});");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_FeedProbe_0 ON FeedProbe ([C0], [C1]);");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_FeedProbe_1 ON FeedProbe ([C2], [C3]);");
        await Exec(con, "CREATE NONCLUSTERED INDEX IX_FeedProbe_2 ON FeedProbe ([C4], [C5]);");
        await Exec(con, $"CREATE TYPE FeedProbeType AS TABLE ({colDefs});");

        Console.WriteLine($"{rows} rows x {columns} columns into a clustered PK + 3 nonclustered table\n");

        await Measure(con, connectionString, "SqlBulkCopy (object[] rows)  ", async () =>
        {
            using var bcp = new SqlBulkCopy(con) { DestinationTableName = "FeedProbe", EnableStreaming = true, BatchSize = 0, BulkCopyTimeout = 600 };
            for (int i = 0; i < columns; i++)
            {
                bcp.ColumnMappings.Add(i, $"C{i}");
            }
            await bcp.WriteToServerAsync(new BoxedReader(rows, columns));
        });

        await Measure(con, connectionString, "table-valued parameter       ", async () =>
        {
            await using var cmd = con.CreateCommand();
            cmd.CommandText = $"INSERT INTO FeedProbe ({colList}) SELECT {colList} FROM @rows;";
            cmd.CommandTimeout = 600;
            var p = cmd.Parameters.AddWithValue("@rows", TvpRows(rows, columns));
            p.SqlDbType = SqlDbType.Structured;
            p.TypeName = "FeedProbeType";
            await cmd.ExecuteNonQueryAsync();
        });

        await Exec(con, "DROP TABLE FeedProbe;");
        await Exec(con, "DROP TYPE FeedProbeType;");
    }

    static async Task Measure(SqlConnection con, string connectionString, string label, Func<Task> action)
    {
        await Exec(con, "TRUNCATE TABLE FeedProbe;");
        GC.Collect();
        GC.WaitForPendingFinalizers();

        var monitor = await GrantMonitor.StartAsync(connectionString);
        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();
        await action();
        sw.Stop();
        double mb = (GC.GetTotalAllocatedBytes(precise: true) - before) / 1024.0 / 1024.0;
        await monitor.DisposeAsync();

        Console.WriteLine($"{label}: {sw.ElapsedMilliseconds,5} ms, {mb,6:F0} MB allocated, " +
            $"grant requested {monitor.MaxRequestedKb / 1024.0,6:F1} MB (required {monitor.MaxRequiredKb / 1024.0:F2} MB)");
    }

    static SqlMetaData Meta(int i) => (i % 5) switch
    {
        0 => new SqlMetaData($"C{i}", SqlDbType.Int),
        1 => new SqlMetaData($"C{i}", SqlDbType.NVarChar, 100),
        2 => new SqlMetaData($"C{i}", SqlDbType.Bit),
        3 => new SqlMetaData($"C{i}", SqlDbType.DateTime2),
        _ => new SqlMetaData($"C{i}", SqlDbType.Decimal, 18, 4),
    };

    // one record, reused for every row, filled through the typed setters — no boxing, no object[]
    static IEnumerable<SqlDataRecord> TvpRows(int rows, int columns)
    {
        var record = new SqlDataRecord(Enumerable.Range(0, columns).Select(Meta).ToArray());
        for (int r = 0; r < rows; r++)
        {
            for (int c = 0; c < columns; c++)
            {
                switch (c % 5)
                {
                    case 0: record.SetInt32(c, r * 31 + c); break;
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

    sealed class BoxedReader : IDataReader
    {
        private readonly int _rows;
        private readonly int _columns;
        private object[] _row = null!;
        private int _index = -1;

        public BoxedReader(int rows, int columns) { _rows = rows; _columns = columns; }

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
