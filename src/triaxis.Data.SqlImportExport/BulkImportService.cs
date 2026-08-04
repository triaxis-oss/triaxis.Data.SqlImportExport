using System.Data;
using System.Data.Common;

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
        bool keepNulls = options?.KeepNulls == true;
        var indexStrategy = options?.IndexStrategy ?? BulkImportIndexStrategy.Maintain;
        int timeout = (int)(options?.Timeout ?? BulkImportOptions.DefaultTimeout).TotalSeconds;
        string grantCap = options?.MaxGrantPercent is int pct ? $"OPTION (MAX_GRANT_PERCENT = {pct})" : "";

        await using var transaction = await sqlConnection.BeginTransactionAsync();

        var schema = await SchemaMetadata.LoadAsync(sqlConnection, transaction);

        // KeepIdentity stays on for every source. When the source supplies the
        // identity column, those values are preserved. When it doesn't, we
        // synthesize them in DataReader using the tracked seed + increment, so
        // SqlBulkCopy still sees a value (SET IDENTITY_INSERT requires it).
        SqlBulkCopyOptions bcpOptions = SqlBulkCopyOptions.KeepIdentity;
        if (keepNulls)
        {
            bcpOptions |= SqlBulkCopyOptions.KeepNulls;
        }

        using var bcp = new SqlBulkCopy(sqlConnection, bcpOptions, (SqlTransaction)transaction)
        {
            BulkCopyTimeout = timeout,
            EnableStreaming = true,
            BatchSize = batchSize,
        };

        var disabledIndexes = new List<string>();
        var insertedIdRanges = new List<InsertedIdRange>();
        var sourceReferenceMap = new Dictionary<IBulkImportSource, Func<int, object>>();

        object ResolveReference(BulkImportSourceReference reference)
        {
            if (!sourceReferenceMap.TryGetValue(reference.Source, out var resolver))
            {
                throw new InvalidOperationException(
                    $"Cannot resolve reference to source '{reference.Source.Name}' - it has not been processed yet, or its reference values were not tracked.");
            }
            return resolver(reference.RowIndex);
        }

        await foreach (var source in input)
        {
            var table = schema[source.Name];
            var strategy = source.Strategy ?? options?.Strategy ?? BulkImportStrategy.Insert;
            bool merge = strategy is BulkImportStrategy.Upsert or BulkImportStrategy.InsertIgnore;

            if (strategy == BulkImportStrategy.Truncate)
            {
                logger.LogWarning("Replacing data in {TableName}", source.Name);
                await sqlConnection.ExecuteAsync($"TRUNCATE TABLE {source.Name}", transaction, timeout);
                // TRUNCATE resets the identity counter back to the seed
                table.ResetIdentity();
            }
            else if (merge)
            {
                logger.LogInformation("Merging data into {TableName} using strategy {Strategy}", source.Name, strategy);
            }
            else
            {
                logger.LogDebug("Importing data into {TableName}", source.Name);
            }

            bool trackIds = !merge;

            var fields = await source.GetColumnNamesAsync();
            var fieldList = fields.ToList();
            var fieldSet = new HashSet<string>(fieldList, StringComparer.OrdinalIgnoreCase);

            Func<int, object>? generateLastColumn = null;
            int captureColumnIndex = -1;
            List<object>? capturedKeys = null;

            if (trackIds)
            {
                if (table.IdentityColumn is string identityColumn && !fieldSet.Contains(identityColumn))
                {
                    long start = await table.GetNextIdentityAsync(sqlConnection, transaction, source.Name);
                    long incr = table.IdentityIncrement;
                    generateLastColumn = i => start + (long)i * incr;
                    fieldList.Add(identityColumn);
                    fieldSet.Add(identityColumn);
                }

                var pkColumns = table.PrimaryKeyColumns;
                if (pkColumns.Count == 1 && fieldSet.Contains(pkColumns[0]))
                {
                    captureColumnIndex = fieldList.FindIndex(f => string.Equals(f, pkColumns[0], StringComparison.OrdinalIgnoreCase));
                    capturedKeys = [];
                }
            }

            if (indexStrategy == BulkImportIndexStrategy.Rebuild && !table.IndexesDisabled && table.SecondaryIndexes.Count > 0)
            {
                table.IndexesDisabled = true;
                logger.LogDebug("Disabling {Count} indexes on {TableName} for the duration of the import", table.SecondaryIndexes.Count, source.Name);
                await sqlConnection.ExecuteAsync(string.Join(";\n",
                    table.SecondaryIndexes.Select(ix => $"ALTER INDEX [{ix}] ON {source.Name} DISABLE")), transaction, timeout);
                disabledIndexes.AddRange(table.SecondaryIndexes.Select(ix => $"ALTER INDEX [{ix}] ON {source.Name} REBUILD"));
            }

            // merging needs somewhere to put the incoming rows before matching them up
            string destination = source.Name;
            if (merge)
            {
                destination = $"#{source.Name}";
                await CreateStagingTableAsync(sqlConnection, transaction, source.Name, replicateDefaults: !keepNulls && table.HasDefaults, timeout);
            }

            await using var reader = source.EnumerateDataAsync().GetAsyncEnumerator();
            using var dataSource = new DataReader(fieldList, reader, ResolveReference, captureColumnIndex, capturedKeys, generateLastColumn);
            bcp.DestinationTableName = destination;
            bcp.ColumnMappings.Clear();
            bcp.ColumnOrderHints.Clear();

            for (int i = 0; i < dataSource.Fields.Length; i++)
            {
                bcp.ColumnMappings.Add(i, dataSource.Fields[i]);
            }

            // The only thing SqlBulkCopy ever tells the server about the incoming rows is how they
            // are sorted - it never sends a row count - so this is the one chance to spare it the
            // clustered index sort and the memory grant that comes with it. The source knows its
            // own order; failing that, we know the order of identity values we generated ourselves.
            // Both are skipped when merging, where the rows land in a heap that has nothing to sort.
            if (!merge)
            {
                foreach (var hint in source.SortedBy)
                {
                    bcp.ColumnOrderHints.Add(hint);
                }

                if (bcp.ColumnOrderHints.Count == 0
                    && generateLastColumn != null && table.IdentityIncrement > 0 && !table.ClusteredKeyDescending
                    && table.ClusteredKeyColumns is [var clusteredKey]
                    && string.Equals(clusteredKey, table.IdentityColumn, StringComparison.OrdinalIgnoreCase))
                {
                    bcp.ColumnOrderHints.Add(clusteredKey, SortOrder.Ascending);
                }
            }

            await bcp.WriteToServerAsync(dataSource);

            if (merge)
            {
                // the staging table carries its own identity values, so moving them across needs
                // the destination's identity column opened up for explicit inserts
                bool identityInsert = table.IdentityColumn != null && fieldSet.Contains(table.IdentityColumn);
                await sqlConnection.ExecuteAsync(
                    MergeSql(source.Name, fields, strategy, table, identityInsert, grantCap), transaction, timeout);
            }

            if (trackIds)
            {
                if (generateLastColumn != null)
                {
                    // we picked every value ourselves, so the next one follows without re-reading it
                    table.NextIdentity = (long)generateLastColumn(dataSource.RowCount);

                    if (dataSource.RowCount > 0)
                    {
                        insertedIdRanges.Add(new InsertedIdRange(source.Name,
                            (long)generateLastColumn(0), (long)generateLastColumn(dataSource.RowCount - 1)));
                    }
                }
                else if (table.IdentityColumn != null && dataSource.RowCount > 0)
                {
                    // the source supplied the identity values, so the counter moved somewhere we
                    // didn't compute - the next synthesis has to read it back
                    table.NextIdentity = null;
                }

                if (dataSource.RowCount > 0)
                {
                    if (capturedKeys != null)
                    {
                        var keys = capturedKeys;
                        sourceReferenceMap[source] = i => keys[i];
                    }
                    else if (generateLastColumn != null)
                    {
                        sourceReferenceMap[source] = generateLastColumn;
                    }
                }
            }
            else if (table.IdentityColumn != null && dataSource.RowCount > 0)
            {
                table.NextIdentity = null;
            }
        }

        if (disabledIndexes.Count > 0)
        {
            logger.LogDebug("Rebuilding {Count} indexes disabled for the import", disabledIndexes.Count);
            await sqlConnection.ExecuteAsync(string.Join(";\n", disabledIndexes), transaction, timeout);
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

    /// <summary>
    /// Creates an empty copy of the table in tempdb. SELECT INTO carries the column types and the
    /// IDENTITY property but not the defaults, which bulk copy needs present to substitute NULLs.
    /// </summary>
    private static Task CreateStagingTableAsync(SqlConnection sqlConnection, DbTransaction transaction, string table, bool replicateDefaults, int timeout)
        => sqlConnection.ExecuteAsync($"""
            IF OBJECT_ID('tempdb..#{table}') IS NOT NULL DROP TABLE [#{table}];
            SELECT TOP 0 * INTO [#{table}] FROM {table};
            {(replicateDefaults ? $"""
            DECLARE @sql nvarchar(max);
            SELECT @sql = N'ALTER TABLE [#{table}] ADD ' + STRING_AGG(CONCAT('CONSTRAINT [', NEWID(), '] DEFAULT ',
                    OBJECT_DEFINITION(default_object_id),
                    ' FOR [', name, ']'), ',')
                FROM sys.columns
                WHERE object_id = OBJECT_ID('{table}') AND default_object_id <> 0;
            IF @sql IS NOT NULL EXEC sp_executesql @sql;
            """ : "")}
            """, transaction, timeout);

    private static string MergeSql(string table, IEnumerable<string> fields, BulkImportStrategy strategy, TableMetadata metadata, bool identityInsert, string grantCap)
    {
        string FormatFields(string prefix = "") => string.Join(", ", fields.Select(f => $"{prefix}[{f}]"));
        string FormatUpdateSet() => string.Join(", ", fields
            .Where(f => !string.Equals(f, metadata.IdentityColumn, StringComparison.OrdinalIgnoreCase))
            .Select(f => $"t.[{f}] = s.[{f}]"));

        return $"""
            DECLARE @condition NVARCHAR(max), @sql NVARCHAR(max);
            {(identityInsert ? $"SET IDENTITY_INSERT [{table}] ON;" : "")}
            SELECT @condition = CONCAT('(', STRING_AGG(s, ') OR ('), ')')
                FROM (select STRING_AGG(CONCAT('s.[', c.name, '] = t.[', c.name, ']'), ' AND ') s from sys.columns c
                INNER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.indexes ix ON ix.object_id = c.object_id AND ic.index_id = ix.index_id
                WHERE c.object_id = OBJECT_ID('{table}')
                GROUP BY ix.index_id) x
            SET @sql = CONCAT(N'MERGE INTO {table} t USING [#{table}] s ON (', @condition, ')
                WHEN NOT MATCHED THEN INSERT ({FormatFields()}) VALUES ({FormatFields("s.")})
                {(strategy == BulkImportStrategy.Upsert ? $"WHEN MATCHED THEN UPDATE SET {FormatUpdateSet()}" : "")}
                {grantCap};
                ');
            EXEC sp_executesql @sql;
            {(identityInsert ? $"SET IDENTITY_INSERT [{table}] OFF;" : "")}
            DROP TABLE [#{table}];
            """;
    }

    private class DataReader : IDataReader
    {
        private readonly string[] _fields;
        private readonly Func<BulkImportSourceReference, object>? _resolveReference;
        private readonly int _captureColumnIndex;
        private readonly List<object>? _captureTarget;
        private readonly Func<int, object>? _generateLastColumn;
        private IAsyncEnumerator<object[]>? _data;
        private object[] _values = null!;
        private object _syntheticValue = null!;

        public DataReader(IEnumerable<string> fields, IAsyncEnumerator<object[]> data, Func<BulkImportSourceReference, object>? resolveReference = null, int captureColumnIndex = -1, List<object>? captureTarget = null, Func<int, object>? generateLastColumn = null)
        {
            _fields = fields.ToArray();
            _data = data;
            _resolveReference = resolveReference;
            _captureColumnIndex = captureColumnIndex;
            _captureTarget = captureTarget;
            _generateLastColumn = generateLastColumn;
        }

        public object this[int i] => i < _values.Length ? _values[i] : _syntheticValue;

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

        public bool GetBoolean(int i) => Convert.ToBoolean(this[i]);
        public byte GetByte(int i) => Convert.ToByte(this[i]);
        public char GetChar(int i) => Convert.ToChar(this[i]);
        public DateTime GetDateTime(int i) => Convert.ToDateTime(this[i]);
        public decimal GetDecimal(int i) => Convert.ToDecimal(this[i]);
        public double GetDouble(int i) => Convert.ToDouble(this[i]);
        public Type GetFieldType(int i) => this[i]?.GetType() ?? DBNull.Value.GetType();
        public float GetFloat(int i) => Convert.ToSingle(this[i]);
        public Guid GetGuid(int i) => (Guid)this[i];
        public short GetInt16(int i) => Convert.ToInt16(this[i]);
        public int GetInt32(int i) => Convert.ToInt32(this[i]);
        public long GetInt64(int i) => Convert.ToInt64(this[i]);
        public string GetString(int i) => this[i].ToString() ?? "";
        public object GetValue(int i) => this[i];

        public int GetValues(object[] values)
        {
            _values.CopyTo(values, 0);
            if (_generateLastColumn != null)
            {
                values[_values.Length] = _syntheticValue;
                return _values.Length + 1;
            }
            return _values.Length;
        }

        public bool IsDBNull(int i) => this[i] == DBNull.Value;

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
            if (_resolveReference != null)
            {
                for (int i = 0; i < _values.Length; i++)
                {
                    if (_values[i] is BulkImportSourceReference reference)
                    {
                        _values[i] = _resolveReference(reference);
                    }
                }
            }
            if (_generateLastColumn != null)
            {
                _syntheticValue = _generateLastColumn(RowCount);
            }
            if (_captureTarget != null && _captureColumnIndex >= 0)
            {
                _captureTarget.Add(this[_captureColumnIndex]);
            }
            RowCount++;
            return true;
        }
    }
}
