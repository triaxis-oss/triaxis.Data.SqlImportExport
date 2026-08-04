using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace Bench;

// Samples what the server actually asks for while the import runs: memory grants and the
// RESOURCE_SEMAPHORE wait that oversized grants queue on.
internal sealed class GrantMonitor : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly SqlConnection _con;

    public long MaxRequestedKb;
    public long MaxIdealKb;
    public long MaxRequiredKb;
    public int Samples;
    public int SamplesWaiting;
    public long SemaphoreWaitMsBefore;
    public long SemaphoreWaitMs;

    private readonly string _connectionString;

    private GrantMonitor(SqlConnection con, string connectionString)
    {
        _con = con;
        _connectionString = connectionString;
        _loop = Task.Run(SampleLoopAsync);
    }

    public static async Task<GrantMonitor> StartAsync(string connectionString)
    {
        long before = await ReadSemaphoreWaitAsync(connectionString);
        var con = new SqlConnection(connectionString);
        await con.OpenAsync();
        return new GrantMonitor(con, connectionString) { SemaphoreWaitMsBefore = before };
    }

    static async Task<long> ReadSemaphoreWaitAsync(string connectionString)
    {
        await using var con = new SqlConnection(connectionString);
        await con.OpenAsync();
        await using var cmd = con.CreateCommand();
        cmd.CommandText = "SELECT ISNULL(SUM(wait_time_ms), 0) FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'RESOURCE_SEMAPHORE%'";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    async Task SampleLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await using var cmd = _con.CreateCommand();
                cmd.CommandText = """
                    SELECT ISNULL(MAX(requested_memory_kb), 0), ISNULL(MAX(ideal_memory_kb), 0),
                           ISNULL(MAX(required_memory_kb), 0), COUNT(*), SUM(IIF(grant_time IS NULL, 1, 0))
                        FROM sys.dm_exec_query_memory_grants;
                    """;
                await using var reader = await cmd.ExecuteReaderAsync(_cts.Token);
                if (await reader.ReadAsync(_cts.Token))
                {
                    MaxRequestedKb = Math.Max(MaxRequestedKb, reader.GetInt64(0));
                    MaxIdealKb = Math.Max(MaxIdealKb, reader.GetInt64(1));
                    MaxRequiredKb = Math.Max(MaxRequiredKb, reader.GetInt64(2));
                    if (reader.GetInt32(3) > 0)
                    {
                        Samples++;
                        if (!reader.IsDBNull(4) && reader.GetInt32(4) > 0)
                        {
                            SamplesWaiting++;
                        }
                    }
                }
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }

            await Task.Delay(15, _cts.Token).ContinueWith(_ => { });
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _loop; } catch { }
        await _con.DisposeAsync();
        SemaphoreWaitMs = await ReadSemaphoreWaitAsync(_connectionString) - SemaphoreWaitMsBefore;
        _cts.Dispose();
    }

    public string Report() =>
        $"grants: max requested {MaxRequestedKb / 1024.0:F1} MB, ideal {MaxIdealKb / 1024.0:F1} MB, required {MaxRequiredKb / 1024.0:F2} MB; " +
        $"samples with a live grant {Samples}, of those ungranted {SamplesWaiting}; RESOURCE_SEMAPHORE waited {SemaphoreWaitMs} ms";
}
