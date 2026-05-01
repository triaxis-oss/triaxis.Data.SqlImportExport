using System.Data;

namespace triaxis.Data.SqlImportExport;

public class BulkImportService(
    ILogger<BulkImportService> logger
) : IBulkImportService
{
    public async Task<IReadOnlyList<InsertedIdRange>> BulkImportAsync(SqlConnection sqlConnection, IAsyncEnumerable<IBulkImportSource> input, BulkImportOptions? options = null)
    {
        if (sqlConnection.State != ConnectionState.Open)
        {
            logger.LogDebug("Opening connection for bulk import");
            // auto open the connection for the duration of the call
            await sqlConnection.OpenAsync();

            try
            {
                return await BulkImportAsync(sqlConnection, input, options);
            }
            finally
            {
                logger.LogDebug("Closing connection after bulk import");
                await sqlConnection.CloseAsync();
            }
        }

        int batchSize = options?.BatchSize ?? BulkImportOptions.DefaultBatchSize;

        await using var transaction = await sqlConnection.BeginTransactionAsync();

        SqlBulkCopyOptions bcpOptions = default;

        if (!(options?.SkipIdentity == true))
        {
            bcpOptions |= SqlBulkCopyOptions.KeepIdentity;
        }
        if (options?.KeepNulls == true)
        {
            bcpOptions |= SqlBulkCopyOptions.KeepNulls;
        }

        using var bcp = new SqlBulkCopy(sqlConnection,
            bcpOptions, (SqlTransaction)transaction)
        {
            BulkCopyTimeout = (int)(options?.Timeout ?? BulkImportOptions.DefaultTimeout).TotalSeconds,
            EnableStreaming = true,
            BatchSize = batchSize,
            NotifyAfter = batchSize,
        };

        var insertedIdRanges = new List<InsertedIdRange>();

        await foreach (var source in input)
        {
            var strategy = source.Strategy ?? options?.Strategy ?? BulkImportStrategy.Insert;
            bool merge = false;
            HashSet<string> identityColumns = [];
            if (strategy == BulkImportStrategy.Truncate)
            {
                logger.LogWarning("Replacing data in {TableName}", source.Name);
                await sqlConnection.ExecuteAsync($"TRUNCATE TABLE {source.Name}", transaction);
            }
            else if (strategy == BulkImportStrategy.Insert)
            {
                logger.LogDebug("Importing data into {TableName}", source.Name);
            }
            else
            {
                merge = true;
                logger.LogInformation("Merging data into {TableName} using strategy {Strategy}", source.Name, strategy);
                identityColumns = (await sqlConnection.QueryAsync<string>($"""
                    IF OBJECT_ID('tempdb..#{source.Name}') IS NOT NULL DROP TABLE #{source.Name};
                    SELECT TOP 0 * INTO #{source.Name} FROM {source.Name};
                    DECLARE @sql nvarchar(max);
                    SELECT @sql = CONCAT(N'ALTER TABLE #{source.Name} ADD ',
                        STRING_AGG(CONCAT('CONSTRAINT [', NEWID(), '] DEFAULT ',
                            OBJECT_DEFINITION(default_object_id),
                            ' FOR [', name, ']'), ','))
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('{source.Name}') AND default_object_id <> 0;
                    EXEC sp_executesql @sql;
                    SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('{source.Name}') AND is_identity = 1;
                    """, transaction
                )).ToHashSet(StringComparer.OrdinalIgnoreCase);
            }

            bool trackIds = !merge && options?.SkipIdentity == true;

            var fields = await source.GetColumnNamesAsync();
            await using var reader = source.EnumerateDataAsync().GetAsyncEnumerator();
            using var dataSource = new DataReader(fields, reader);
            bcp.DestinationTableName = merge ? $"#{source.Name}" : source.Name;
            bcp.ColumnMappings.Clear();

            for (int i = 0; i < dataSource.Fields.Length; i++)
            {
                bcp.ColumnMappings.Add(i, dataSource.Fields[i]);
            }

            await bcp.WriteToServerAsync(dataSource);

            if (merge)
            {
                string FormatFields(string prefix = "") => string.Join(", ", fields.Select(f => $"{prefix}[{f}]"));
                string FormatUpdateSet() => string.Join(", ", fields.Where(f => !identityColumns.Contains(f)).Select(f => $"t.[{f}] = s.[{f}]"));

                // perform the actual merge and drop the temp table
                var sql = $"""
                    DECLARE @condition NVARCHAR(max), @sql NVARCHAR(max);
                    {(identityColumns.Any() ? $"SET IDENTITY_INSERT [{source.Name}] ON;" : "")}
                    SELECT @condition = CONCAT('(', STRING_AGG(s, ') OR ('), ')')
                        FROM (select STRING_AGG(CONCAT('s.[', c.name, '] = t.[', c.name, ']'), ' AND ') s from sys.columns c
                        INNER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        INNER JOIN sys.indexes ix ON ix.object_id = c.object_id AND ic.index_id = ix.index_id
                        WHERE c.object_id = OBJECT_ID('{source.Name}')
                        GROUP BY ix.index_id) x
                    SET @sql = CONCAT(N'MERGE INTO {source.Name} t USING [#{source.Name}] s ON (', @condition, ')
                        WHEN NOT MATCHED THEN INSERT ({FormatFields()}) VALUES ({FormatFields("s.")})
                        {(strategy == BulkImportStrategy.Upsert ? $"WHEN MATCHED THEN UPDATE SET {FormatUpdateSet()}" : "")};
                        ');
                    EXEC sp_executesql @sql;
                    {(identityColumns.Any() ? $"SET IDENTITY_INSERT [{source.Name}] OFF;" : "")}
                    DROP TABLE [#{source.Name}];
                    """;

                await sqlConnection.ExecuteAsync(sql, transaction);
            }

            if (trackIds && dataSource.RowCount > 0)
            {
                var identInfo = (await sqlConnection.QueryAsync<(long Incr, long LastId)>(
                    $"SELECT CONVERT(bigint, increment_value), CONVERT(bigint, IDENT_CURRENT(N'{source.Name}')) FROM sys.identity_columns WHERE object_id = OBJECT_ID(N'{source.Name}')",
                    transaction)).ToList();
                if (identInfo.Count > 0)
                {
                    var (incr, lastId) = identInfo[0];
                    insertedIdRanges.Add(new InsertedIdRange(source.Name, lastId - (long)(dataSource.RowCount - 1) * incr, lastId));
                }
            }
        }

        if (!(options?.SkipConstraints == true))
        {
            var tables = await sqlConnection.QueryAsync<string>("SELECT DISTINCT OBJECT_NAME(parent_object_id) FROM sys.foreign_keys WHERE is_not_trusted = 1", transaction);
            int verifyCnt = tables.Count();
            if (verifyCnt > 0)
            {
                logger.LogDebug("Verifying constraits in {Count} tables", verifyCnt);
                var sql = string.Join(";\n",
                    tables.Select(tbl => $"ALTER TABLE [{tbl}] WITH CHECK CHECK CONSTRAINT ALL"));
                await sqlConnection.ExecuteAsync(sql, transaction);
            }
            else
            {
                logger.LogDebug("No constraints to verify");
            }
        }

        if (options?.DryRun == true)
        {
            await transaction.RollbackAsync();
        }
        else
        {
            await transaction.CommitAsync();
        }

        return insertedIdRanges;
    }

    private class DataReader : IDataReader
    {
        private readonly string[] _fields;
        private IAsyncEnumerator<object[]>? _data;
        private object[] _values = null!;

        public DataReader(IEnumerable<string> fields, IAsyncEnumerator<object[]> data)
        {
            _fields = fields.ToArray();
            _data = data;
        }

        public object this[int i] => _values[i];

        public object this[string name] => throw new NotImplementedException();

        public int Depth => 0;
        public bool IsClosed => _data == null;
        public int RecordsAffected => 0;
        public int FieldCount => _fields.Length;
        public string[] Fields => _fields;
        public int RowCount { get; private set; }

        public void Close() { _data = null; }
        public void Dispose() { Close(); }

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
            => throw new NotImplementedException();
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
            => throw new NotImplementedException();

        public IDataReader GetData(int i) => throw new NotImplementedException();
        public string GetDataTypeName(int i) => throw new NotImplementedException();
        public DataTable? GetSchemaTable() => throw new NotImplementedException();

        public bool GetBoolean(int i) => Convert.ToBoolean(_values[i]);
        public byte GetByte(int i) => Convert.ToByte(_values[i]);
        public char GetChar(int i) => Convert.ToChar(_values[i]);
        public DateTime GetDateTime(int i) => Convert.ToDateTime(_values[i]);
        public decimal GetDecimal(int i) => Convert.ToDecimal(_values[i]);
        public double GetDouble(int i) => Convert.ToDouble(_values[i]);
        public Type GetFieldType(int i) => _values[i]?.GetType() ?? DBNull.Value.GetType();
        public float GetFloat(int i) => Convert.ToSingle(_values[i]);
        public Guid GetGuid(int i) => (Guid)_values[i];
        public short GetInt16(int i) => Convert.ToInt16(_values[i]);
        public int GetInt32(int i) => Convert.ToInt32(_values[i]);
        public long GetInt64(int i) => Convert.ToInt64(_values[i]);
        public string GetString(int i) => _values[i].ToString() ?? "";
        public object GetValue(int i) => _values[i];

        public int GetValues(object[] values)
        {
            _values.CopyTo(values, 0);
            return _values.Length;
        }

        public bool IsDBNull(int i) => _values[i] == DBNull.Value;

        public string GetName(int i) => _fields[i];
        public int GetOrdinal(string name)
        {
            for (int i = 0; i < _fields.Length; i++)
            {
                if (_fields[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }

        public bool NextResult() => false;

        public bool Read()
        {
            if (_data == null)
            {
                return false;
            }

            // TODO: maybe implement DbDataReader which has async overloads for Read as well
            if (!_data.MoveNextAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult())
            {
                Close();
                return false;
            }

            _values = _data.Current;
            RowCount++;
            return true;
        }
    }
}
