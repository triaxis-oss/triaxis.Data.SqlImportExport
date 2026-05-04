using System.Data.Common;

namespace triaxis.Data.SqlImportExport;

internal static class PrivateExtensions
{

    /// <summary>
    /// Quick and dirty single-column or tuple-mapping query
    /// </summary>
    public static async Task<IEnumerable<T>> QueryAsync<T>(this SqlConnection sqlConnection, string query, DbTransaction? transaction = null)
    {
        await using var cmd = CreateCommand(sqlConnection, query, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        return await ReadAllAsync<T>(reader);
    }

    /// <summary>
    /// Two-result-set variant of <see cref="QueryAsync{T}"/> — runs both selects in a single round trip.
    /// </summary>
    public static async Task<(IEnumerable<T1> First, IEnumerable<T2> Second)> QueryAsync<T1, T2>(this SqlConnection sqlConnection, string query, DbTransaction? transaction = null)
    {
        await using var cmd = CreateCommand(sqlConnection, query, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        var first = await ReadAllAsync<T1>(reader);
        await reader.NextResultAsync();
        var second = await ReadAllAsync<T2>(reader);
        return (first, second);
    }

    private static SqlCommand CreateCommand(SqlConnection sqlConnection, string query, DbTransaction? transaction)
    {
        var cmd = sqlConnection.CreateCommand();
        cmd.CommandText = query;
        if (transaction is SqlTransaction sqlTransaction)
        {
            cmd.Transaction = sqlTransaction;
        }
        return cmd;
    }

    private static async Task<List<T>> ReadAllAsync<T>(DbDataReader reader)
    {
        var result = new List<T>();
        if (reader.FieldCount == 0)
        {
            return result;
        }

        var values = new object?[reader.FieldCount];
        while (await reader.ReadAsync())
        {
            reader.GetValues(values);
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == DBNull.Value) { values[i] = null; }
            }

            if (values[0] is T value)
            {
                result.Add(value);
            }
            else
            {
                result.Add((T)Activator.CreateInstance(typeof(T), values)!);
            }
        }

        return result;
    }

    public static async Task<int> ExecuteAsync(this SqlConnection sqlConnection, string command, DbTransaction? transaction = null)
    {
        await using var cmd = sqlConnection.CreateCommand();
        cmd.CommandText = command;
        if (transaction is SqlTransaction sqlTransaction)
        {
            cmd.Transaction = sqlTransaction;
        }
        return await cmd.ExecuteNonQueryAsync();
    }
}
