using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace triaxis.Data.SqlImportExport;

internal static class PrivateExtensions
{
    private const DynamicallyAccessedMemberTypes Bindable = DynamicallyAccessedMemberTypes.PublicConstructors;

    /// <summary>
    /// Quick and dirty single-column or tuple-mapping query
    /// </summary>
    /// <remarks>
    /// Rows are bound by invoking the constructor, which a trimmer has no way to see. The
    /// annotation is what keeps it: it roots the constructors of whatever is asked for here, so
    /// the binding still works in a trimmed application.
    /// </remarks>
    public static async Task<IEnumerable<T>> QueryAsync<[DynamicallyAccessedMembers(Bindable)] T>(this SqlConnection sqlConnection, string query, DbTransaction? transaction = null, params (string Name, object Value)[] parameters)
    {
        await using var cmd = CreateCommand(sqlConnection, query, transaction, parameters);
        using var reader = await cmd.ExecuteReaderAsync();
        return await ReadAllAsync<T>(reader);
    }

    /// <summary>
    /// Two-result-set variant of <see cref="QueryAsync{T}"/> — runs both selects in a single round trip.
    /// </summary>
    public static async Task<(IEnumerable<T1> First, IEnumerable<T2> Second)> QueryAsync<[DynamicallyAccessedMembers(Bindable)] T1, [DynamicallyAccessedMembers(Bindable)] T2>(this SqlConnection sqlConnection, string query, DbTransaction? transaction = null)
    {
        await using var cmd = CreateCommand(sqlConnection, query, transaction, []);
        using var reader = await cmd.ExecuteReaderAsync();
        var first = await ReadAllAsync<T1>(reader);
        await reader.NextResultAsync();
        var second = await ReadAllAsync<T2>(reader);
        return (first, second);
    }

    private static SqlCommand CreateCommand(SqlConnection sqlConnection, string query, DbTransaction? transaction, (string Name, object Value)[] parameters)
    {
        var cmd = sqlConnection.CreateCommand();
        cmd.CommandText = query;
        if (transaction is SqlTransaction sqlTransaction)
        {
            cmd.Transaction = sqlTransaction;
        }
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }
        return cmd;
    }

    private static async Task<List<T>> ReadAllAsync<[DynamicallyAccessedMembers(Bindable)] T>(DbDataReader reader)
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

    public static async Task<int> ExecuteAsync(this SqlConnection sqlConnection, string command, DbTransaction? transaction = null, int? timeout = null)
    {
        await using var cmd = sqlConnection.CreateCommand();
        cmd.CommandText = command;
        if (transaction is SqlTransaction sqlTransaction)
        {
            cmd.Transaction = sqlTransaction;
        }
        if (timeout is int seconds)
        {
            cmd.CommandTimeout = seconds;
        }
        return await cmd.ExecuteNonQueryAsync();
    }
}
