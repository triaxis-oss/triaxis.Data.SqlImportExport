using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace Bench;

// Does SqlBulkCopy ever call the typed accessors on a plain IDataReader, or does it always
// box through GetValue? Determines whether a typed source path can avoid any allocation at all.
internal static class TypedProbe
{
    const string Base = "Server=localhost,1433;User Id=sa;Password=YourStrong@Passw0rd;Encrypt=False;TrustServerCertificate=True;";

    public static async Task RunAsync(string connectionString, int rows, int columns)
    {
        await using var con = new SqlConnection(connectionString);
        await con.OpenAsync();

        await Exec(con, "IF OBJECT_ID('AllocProbe') IS NOT NULL DROP TABLE AllocProbe;");
        var cols = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}] {Program.ColumnType(i)} NULL"));
        await Exec(con, $"CREATE TABLE AllocProbe ({cols});");

        foreach (var typed in new[] { false, true })
        {
            await Exec(con, "TRUNCATE TABLE AllocProbe;");
            var reader = new ColumnarReader(rows, columns, typed);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            long before = GC.GetTotalAllocatedBytes(precise: true);
            var sw = Stopwatch.StartNew();

            using (var bcp = new SqlBulkCopy(con) { DestinationTableName = "AllocProbe", EnableStreaming = true, BatchSize = 0, BulkCopyTimeout = 300 })
            {
                for (int i = 0; i < columns; i++)
                {
                    bcp.ColumnMappings.Add(i, $"C{i}");
                }
                await bcp.WriteToServerAsync(reader);
            }

            sw.Stop();
            double mb = (GC.GetTotalAllocatedBytes(precise: true) - before) / 1024.0 / 1024.0;
            Console.WriteLine($"{(typed ? "typed columnar reader " : "object[] materialising")}: {mb:F0} MB, {sw.ElapsedMilliseconds} ms  " +
                $"(GetValue calls {reader.GetValueCalls}, typed-accessor calls {reader.TypedCalls})");
        }

        await Exec(con, "DROP TABLE AllocProbe;");
    }

    static async Task Exec(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync();
    }

    // Holds data the way a columnar Parquet read would: typed arrays, no per-row object[].
    // In object[] mode it materialises a row per Read() the way the current source contract forces.
    sealed class ColumnarReader : IDataReader
    {
        private readonly int _rows;
        private readonly int _columns;
        private readonly bool _typed;
        private readonly int[] _ints;
        private readonly string[] _strings;
        private readonly bool[] _bools;
        private readonly DateTime[] _dates;
        private readonly decimal[] _decimals;
        private object[]? _row;
        private int _index = -1;

        public long GetValueCalls;
        public long TypedCalls;

        public ColumnarReader(int rows, int columns, bool typed)
        {
            _rows = rows;
            _columns = columns;
            _typed = typed;

            // one page of source values, indexed modulo — the point is the access pattern, not the data
            _ints = Enumerable.Range(0, 1000).ToArray();
            _strings = Enumerable.Range(0, 1000).Select(i => $"value-{i}").ToArray();
            _bools = Enumerable.Range(0, 1000).Select(i => i % 2 == 0).ToArray();
            _dates = Enumerable.Range(0, 1000).Select(i => new DateTime(2020, 1, 1).AddMinutes(i)).ToArray();
            _decimals = Enumerable.Range(0, 1000).Select(i => decimal.Divide(i, 7)).ToArray();
        }

        object Raw(int i) => (i % 5) switch
        {
            0 => _ints[(_index + i) % 1000],
            1 => _strings[(_index + i) % 1000],
            2 => _bools[(_index + i) % 1000],
            3 => _dates[(_index + i) % 1000],
            _ => _decimals[(_index + i) % 1000],
        };

        public bool Read()
        {
            if (++_index >= _rows)
            {
                return false;
            }
            if (!_typed)
            {
                _row = new object[_columns];
                for (int i = 0; i < _columns; i++)
                {
                    _row[i] = Raw(i);
                }
            }
            return true;
        }

        public object GetValue(int i)
        {
            GetValueCalls++;
            return _typed ? Raw(i) : _row![i];
        }

        public int GetInt32(int i) { TypedCalls++; return _ints[(_index + i) % 1000]; }
        public string GetString(int i) { TypedCalls++; return _strings[(_index + i) % 1000]; }
        public bool GetBoolean(int i) { TypedCalls++; return _bools[(_index + i) % 1000]; }
        public DateTime GetDateTime(int i) { TypedCalls++; return _dates[(_index + i) % 1000]; }
        public decimal GetDecimal(int i) { TypedCalls++; return _decimals[(_index + i) % 1000]; }

        public int FieldCount => _columns;
        public string GetName(int i) => $"C{i}";
        public int GetOrdinal(string name) => int.Parse(name.Substring(1));
        public Type GetFieldType(int i) => (i % 5) switch
        {
            0 => typeof(int),
            1 => typeof(string),
            2 => typeof(bool),
            3 => typeof(DateTime),
            _ => typeof(decimal),
        };
        public bool IsDBNull(int i) => false;
        public int GetValues(object[] values)
        {
            for (int i = 0; i < _columns; i++) { values[i] = GetValue(i); }
            return _columns;
        }
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => 0;
        public void Close() { }
        public void Dispose() { }
        public bool NextResult() => false;
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long o, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long o, char[]? b, int bo, int l) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public double GetDouble(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public long GetInt64(int i) => throw new NotSupportedException();
        public DataTable? GetSchemaTable() => null;
    }
}
