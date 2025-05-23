
using System.Data;
using System.Text;

namespace triaxis.Data.SqlImportExport;

public class BulkExportService(
    ILogger<BulkExportService> logger
) : IBulkExportService
{
    public async IAsyncEnumerable<IBulkExportTable> BulkExportAsync(SqlConnection sqlConnection, BulkExportOptions? options = null)
    {
        if (sqlConnection.State != ConnectionState.Open)
        {
            logger.LogDebug("Opening connection for bulk export");
            // auto open the connection for the duration of the call
            await sqlConnection.OpenAsync();

            try
            {
                await foreach (var res in BulkExportAsync(sqlConnection, options))
                {
                    yield return res;
                }
                yield break;
            }
            finally
            {
                logger.LogDebug("Closing connection after bulk export");
                await sqlConnection.CloseAsync();
            }
        }

        logger.LogDebug("Retrieving table names for bulk export");
        var tableNames = (await sqlConnection.QueryAsync<string>("""
            SELECT name FROM sys.tables
            WHERE type = 'U' AND is_ms_shipped = 0
            ORDER BY name
            """))
            .Where(t => options?.TableFilter?.Invoke(t) != false)
            .ToList();

        logger.LogDebug("Going to export data for {TableCount} tables", tableNames.Count);

        var tableColumns = (await sqlConnection.QueryAsync<(string table, string name, string type, string def)>("""
            SELECT t.name, c.name, type.name, OBJECT_DEFINITION(c.default_object_id)
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.types type ON type.user_type_id = c.user_type_id
            OUTER APPLY (
                SELECT TOP 1 1 is_pk
                FROM sys.index_columns ic
                INNER JOIN sys.indexes ix ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
                WHERE ix.is_primary_key = 1 AND ic.object_id = c.object_id AND ic.column_id = c.column_id
            ) ix
            ORDER BY t.name, c.object_id, IIF(ix.is_pk = 1, 0, 1), c.column_id
            """))
            .ToLookup(r => r.table, r => (r.name, def: ParseDefault(r.type, r.def)));

        var queryAll = new StringBuilder();

        queryAll.Append("SET TRANSACTION ISOLATION LEVEL SNAPSHOT;\n");
        queryAll.Append("SET NOCOUNT ON;\n");
        queryAll.Append("SET XACT_ABORT ON;\n");
        queryAll.Append("BEGIN TRANSACTION;\n");

        var queries = tableNames
            .Where(t => options?.TableFilter?.Invoke(t) != false)
            .Select(table => (table, columns: tableColumns[table]
                .Where(c => options?.ColumnFilter?.Invoke((table, c.name)) != false)
                .ToList()))
            .Where(tc => tc.columns.Count > 0)
            .ToList();

        foreach (var (table, columns) in queries)
        {
            queryAll.Append("SELECT ");
            for (int i = 0; i < columns.Count; i++)
            {
                if (i > 0) { queryAll.Append(", "); }
                queryAll.Append('[').Append(columns[i].name).Append(']');
            }
            queryAll.Append(" FROM [").Append(table).Append("] WITH (NOLOCK);\n");
        }

        queryAll.Append("ROLLBACK TRANSACTION");

        await using var cmd = sqlConnection.CreateCommand();
        cmd.CommandText = queryAll.ToString();
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        for (int i = 0; i < queries.Count; i++)
        {
            var (table, columns) = queries[i];

            if (i > 0)
            {
                await reader.NextResultAsync();
            }

            var cols = columns.Select((c, i) => new TableColumn(c.name, reader.GetFieldType(i), c.def)).ToList();
            var tbl = new Table(reader, table, cols);
            yield return tbl;
            await tbl.FlushAsync();
        }
    }

    class Table(SqlDataReader reader, string table, IReadOnlyList<IBulkExportColumn> columns) : IBulkExportTable
    {
        private bool _done;

        public string Name => table;

        public IReadOnlyList<IBulkExportColumn> Columns => columns;

        public async IAsyncEnumerable<IEnumerable<object?>> GetRowsAsync()
        {
            while (!_done)
            {
                if (!await reader.ReadAsync())
                {
                    _done = true;
                    break;
                }
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                yield return values;
            }
        }

        public async ValueTask FlushAsync()
        {
            while (!_done)
            {
                if (!await reader.ReadAsync())
                {
                    _done = true;
                    break;
                }
            }
        }
    }

    class TableColumn(string name, Type type, object? defaultValue) : IBulkExportColumn
    {
        public string Name => name;
        public Type Type => type;
        public object? DefaultValue => defaultValue;
    }

    static object? ParseDefault(string type, string? defaultValue)
    {
        if (defaultValue is null)
        {
            return null;
        }

        while (defaultValue.StartsWith("(") && defaultValue.EndsWith(")"))
        {
            defaultValue = defaultValue[1..^1];
        }

        if (int.TryParse(defaultValue, out var intValue))
        {
            if (type == "bit")
            {
                return intValue == 1;
            }
            return intValue;
        }

        if (defaultValue.StartsWith("'") && defaultValue.EndsWith("'"))
        {
            defaultValue = defaultValue[1..^1];
        }

        return defaultValue;
    }
}
