namespace triaxis.Data.SqlImportExport;

internal static class PrivateExtensions
{

    /// <summary>
    /// Quick and dirty single-column or tuple-mapping query
    /// </summary>
    public static async Task<IEnumerable<T>> QueryAsync<T>(this SqlConnection sqlConnection, string query)
    {
        await using var cmd = sqlConnection.CreateCommand();
        var result = new List<T>();
        cmd.CommandText = query;

        using var reader = await cmd.ExecuteReaderAsync();
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

    public static async Task<int> ExecuteAsync(this SqlConnection sqlConnection, string command)
    {
        await using var cmd = sqlConnection.CreateCommand();
        cmd.CommandText = command;
        return await cmd.ExecuteNonQueryAsync();
    }
}
