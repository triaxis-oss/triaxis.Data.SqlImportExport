using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace Bench;

// Component-level probe: attributes the per-table cost of a near-empty load to the
// metadata round trip, the bulk copy itself, and the staging round trips.
internal static class Probe
{
    const string Base = "Server=localhost,1433;User Id=sa;Password=YourStrong@Passw0rd;Encrypt=False;TrustServerCertificate=True;";
    const string DbName = "BulkImportBench";

    public static async Task RunAsync(int tables)
    {
        var cs = new SqlConnectionStringBuilder(Base) { InitialCatalog = DbName }.ToString();
        await using var con = new SqlConnection(cs);
        await con.OpenAsync();

        var names = Enumerable.Range(0, tables).Select(i => $"Small{i}").ToList();

        // warm the connection/plan cache before any timing
        await Time(con, names.Take(5).ToList(), InterpolatedMetadataAsync);

        Console.WriteLine($"interpolated metadata query : {await Time(con, names, InterpolatedMetadataAsync):F0} ms  ({tables} tables)");
        Console.WriteLine($"parameterised metadata query: {await Time(con, names, ParameterisedMetadataAsync):F0} ms");
        Console.WriteLine($"single batched metadata     : {await TimeOnce(con, BatchedMetadataAsync):F0} ms  (one query, all tables)");
        Console.WriteLine($"empty SqlBulkCopy (5 rows)  : {await Time(con, names, BulkCopyAsync):F0} ms");
        Console.WriteLine($"staging create+copy+drop    : {await Time(con, names, StagingAsync):F0} ms");
    }

    static async Task<double> Time(SqlConnection con, List<string> names, Func<SqlConnection, string, Task> op)
    {
        var sw = Stopwatch.StartNew();
        foreach (var n in names)
        {
            await op(con, n);
        }
        return sw.Elapsed.TotalMilliseconds;
    }

    static async Task<double> TimeOnce(SqlConnection con, Func<SqlConnection, Task> op)
    {
        var sw = Stopwatch.StartNew();
        await op(con);
        return sw.Elapsed.TotalMilliseconds;
    }

    static async Task InterpolatedMetadataAsync(SqlConnection con, string table)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = $"""
            SELECT name, CONVERT(bigint, increment_value),
                ISNULL(CONVERT(bigint, last_value) + CONVERT(bigint, increment_value), CONVERT(bigint, seed_value))
                FROM sys.identity_columns WHERE object_id = OBJECT_ID(N'{table}');

            SELECT c.name
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
                WHERE i.object_id = OBJECT_ID(N'{table}') AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal;
            """;
        await Drain(cmd);
    }

    static async Task ParameterisedMetadataAsync(SqlConnection con, string table)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = """
            DECLARE @id int = OBJECT_ID(@table);
            SELECT name, CONVERT(bigint, increment_value),
                ISNULL(CONVERT(bigint, last_value) + CONVERT(bigint, increment_value), CONVERT(bigint, seed_value))
                FROM sys.identity_columns WHERE object_id = @id;

            SELECT c.name
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
                WHERE i.object_id = @id AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal;
            """;
        cmd.Parameters.AddWithValue("@table", table);
        await Drain(cmd);
    }

    static async Task BatchedMetadataAsync(SqlConnection con)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = """
            SELECT OBJECT_NAME(object_id), name, CONVERT(bigint, increment_value),
                ISNULL(CONVERT(bigint, last_value) + CONVERT(bigint, increment_value), CONVERT(bigint, seed_value))
                FROM sys.identity_columns;

            SELECT OBJECT_NAME(i.object_id), c.name
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
                WHERE i.is_primary_key = 1
                ORDER BY ic.key_ordinal;
            """;
        await Drain(cmd);
    }

    static async Task Drain(SqlCommand cmd)
    {
        await using var reader = await cmd.ExecuteReaderAsync();
        do
        {
            while (await reader.ReadAsync()) { }
        } while (await reader.NextResultAsync());
    }

    static async Task BulkCopyAsync(SqlConnection con, string table)
    {
        using var bcp = new SqlBulkCopy(con) { DestinationTableName = table, EnableStreaming = true };
        bcp.ColumnMappings.Add(0, "C0");
        await bcp.WriteToServerAsync(new FiveIntRows());
    }

    static async Task StagingAsync(SqlConnection con, string table)
    {
        await using var create = con.CreateCommand();
        create.CommandText = $"SELECT TOP 0 * INTO [#stg_{table}] FROM [{table}];";
        await create.ExecuteNonQueryAsync();

        using (var bcp = new SqlBulkCopy(con) { DestinationTableName = $"[#stg_{table}]", EnableStreaming = true })
        {
            bcp.ColumnMappings.Add(0, "C0");
            await bcp.WriteToServerAsync(new FiveIntRows());
        }

        await using var move = con.CreateCommand();
        move.CommandText = $"INSERT INTO [{table}] ([C0]) SELECT [C0] FROM [#stg_{table}]; DROP TABLE [#stg_{table}];";
        await move.ExecuteNonQueryAsync();
    }

    sealed class FiveIntRows : IDataReader
    {
        int _row;
        public int FieldCount => 1;
        public object GetValue(int i) => _row * 7;
        public bool Read() => _row++ < 5;
        public string GetName(int i) => "C0";
        public int GetOrdinal(string name) => 0;
        public Type GetFieldType(int i) => typeof(int);
        public bool IsDBNull(int i) => false;
        public int GetValues(object[] values) { values[0] = GetValue(0); return 1; }
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(0);
        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => 0;
        public void Close() { }
        public void Dispose() { }
        public bool NextResult() => false;
        public bool GetBoolean(int i) => throw new NotSupportedException();
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long o, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long o, char[]? b, int bo, int l) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public string GetDataTypeName(int i) => throw new NotSupportedException();
        public DateTime GetDateTime(int i) => throw new NotSupportedException();
        public decimal GetDecimal(int i) => throw new NotSupportedException();
        public double GetDouble(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => throw new NotSupportedException();
        public string GetString(int i) => throw new NotSupportedException();
        public DataTable? GetSchemaTable() => null;
    }
}
