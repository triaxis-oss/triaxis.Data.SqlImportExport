using System.Data;

namespace triaxis.Data.SqlImportExport.Tests;

internal sealed class ListSource : IBulkImportSource
{
    private readonly string[] _columns;
    private readonly object?[][] _rows;

    public ListSource(string name, string[] columns, params object?[][] rows)
    {
        Name = name;
        _columns = columns;
        _rows = rows;
    }

    public string Name { get; }
    public BulkImportStrategy? Strategy { get; init; }
    public IEnumerable<SqlBulkCopyColumnOrderHint> SortedBy { get; init; } = [];

    public Task<IEnumerable<string>> GetColumnNamesAsync()
        => Task.FromResult<IEnumerable<string>>(_columns);

    public async IAsyncEnumerable<object?[]> EnumerateDataAsync()
    {
        await Task.CompletedTask;
        foreach (var r in _rows)
        {
            yield return r;
        }
    }
}

internal sealed class DelegateSource(string name, string[] columns, Func<IAsyncEnumerable<object?[]>> rows) : IBulkImportSource
{
    public string Name => name;

    public Task<IEnumerable<string>> GetColumnNamesAsync()
        => Task.FromResult<IEnumerable<string>>(columns);

    public IAsyncEnumerable<object?[]> EnumerateDataAsync() => rows();
}

internal static class TestHelpers
{
    public static async IAsyncEnumerable<IBulkImportSource> AsAsync(params IBulkImportSource[] sources)
    {
        await Task.CompletedTask;
        foreach (var s in sources)
        {
            yield return s;
        }
    }

    public static async Task ExecAsync(this SqlConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<List<T>> ReadAsync<T>(this SqlConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();

        // only use ctor invocation for multi-column reads (records / tuples).
        // for single-column reads, picking up e.g. string's char[] ctor would corrupt the value.
        var ctor = reader.FieldCount > 1
            ? typeof(T).GetConstructors().FirstOrDefault(c => c.GetParameters().Length == reader.FieldCount)
            : null;
        var paramTypes = ctor?.GetParameters().Select(p => p.ParameterType).ToArray();

        var result = new List<T>();
        while (await reader.ReadAsync())
        {
            var values = new object?[reader.FieldCount];
            reader.GetValues(values!);
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == DBNull.Value) values[i] = null;
                else if (paramTypes != null && values[i] != null)
                {
                    var target = Nullable.GetUnderlyingType(paramTypes[i]) ?? paramTypes[i];
                    if (values[i]!.GetType() != target)
                    {
                        values[i] = Convert.ChangeType(values[i], target);
                    }
                }
            }

            if (ctor != null)
            {
                result.Add((T)ctor.Invoke(values));
            }
            else
            {
                var val = values[0];
                if (val != null && val.GetType() != typeof(T))
                {
                    var target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                    if (val.GetType() != target)
                    {
                        val = Convert.ChangeType(val, target);
                    }
                }
                result.Add((T)val!);
            }
        }
        return result;
    }
}

public abstract class SqlTestFixture
{
    private const string DefaultConnectionString =
        "Server=localhost,1433;User Id=sa;Password=YourStrong@Passw0rd;Encrypt=False;TrustServerCertificate=True;";

    private static string BaseConnectionString =>
        Environment.GetEnvironmentVariable("TEST_SQL_CONNECTION_STRING") ?? DefaultConnectionString;

    private static string MasterConnectionString =>
        new SqlConnectionStringBuilder(BaseConnectionString) { InitialCatalog = "master" }.ToString();

    private string DatabaseName => $"BulkImportTests_{GetType().Name}";

    protected string TestConnectionString =>
        new SqlConnectionStringBuilder(BaseConnectionString) { InitialCatalog = DatabaseName }.ToString();

    protected SqlConnection Connection { get; private set; } = null!;
    protected BulkImportService Service { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task FixtureSetup()
    {
        await WaitForServerAsync();
        await using var master = new SqlConnection(MasterConnectionString);
        await master.OpenAsync();
        await master.ExecAsync($"""
            IF DB_ID('{DatabaseName}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{DatabaseName}];
            END;
            """);
        await master.ExecAsync($"CREATE DATABASE [{DatabaseName}];");
    }

    [OneTimeTearDown]
    public async Task FixtureTearDown()
    {
        try
        {
            await using var master = new SqlConnection(MasterConnectionString);
            await master.OpenAsync();
            await master.ExecAsync($"""
                IF DB_ID('{DatabaseName}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{DatabaseName}];
                END;
                """);
        }
        catch
        {
            // best-effort cleanup
        }
    }

    [SetUp]
    public async Task PerTestSetup()
    {
        Connection = new SqlConnection(TestConnectionString);
        await Connection.OpenAsync();
        Service = new BulkImportService(NullLogger<BulkImportService>.Instance);
    }

    [TearDown]
    public async Task PerTestTearDown()
    {
        if (Connection.State == ConnectionState.Open)
        {
            // drop user FKs first, then tables — leave the DB itself
            await Connection.ExecAsync("""
                DECLARE @sql nvarchar(max) = N'';
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + N'].[' + OBJECT_NAME(parent_object_id) + N'] DROP CONSTRAINT [' + name + N']; '
                    FROM sys.foreign_keys;
                EXEC sp_executesql @sql;

                SET @sql = N'';
                SELECT @sql += N'DROP TABLE [' + OBJECT_SCHEMA_NAME(object_id) + N'].[' + name + N']; '
                    FROM sys.tables;
                EXEC sp_executesql @sql;
                """);
        }
        await Connection.DisposeAsync();
    }

    private static async Task WaitForServerAsync()
    {
        var deadline = DateTimeOffset.UtcNow.AddMinutes(2);
        while (true)
        {
            try
            {
                await using var probe = new SqlConnection(MasterConnectionString);
                await probe.OpenAsync();
                return;
            }
            catch (SqlException) when (DateTimeOffset.UtcNow < deadline)
            {
                await Task.Delay(2000);
            }
        }
    }
}
