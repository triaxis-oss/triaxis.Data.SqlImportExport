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

        // KeepIdentity stays on for every source. When the source supplies the
        // identity column, those values are preserved. When it doesn't, we
        // synthesize them in DataReader using IDENT_CURRENT + increment, so
        // SqlBulkCopy still sees a value (SET IDENTITY_INSERT requires it).
        SqlBulkCopyOptions bcpOptions = SqlBulkCopyOptions.KeepIdentity;
        if (options?.KeepNulls == true)
        {
            bcpOptions |= SqlBulkCopyOptions.KeepNulls;
        }

        using var bcp = new SqlBulkCopy(sqlConnection, bcpOptions, (SqlTransaction)transaction)
        {
            BulkCopyTimeout = (int)(options?.Timeout ?? BulkImportOptions.DefaultTimeout).TotalSeconds,
            EnableStreaming = true,
            BatchSize = batchSize,
            NotifyAfter = batchSize,
        };

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
                    SELECT @sql = N'ALTER TABLE #{source.Name} ADD ' + STRING_AGG(CONCAT('CONSTRAINT [', NEWID(), '] DEFAULT ',
                            OBJECT_DEFINITION(default_object_id),
                            ' FOR [', name, ']'), ',')
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('{source.Name}') AND default_object_id <> 0;
                    IF @sql IS NOT NULL EXEC sp_executesql @sql;
                    SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('{source.Name}') AND is_identity = 1;
                    """, transaction
                )).ToHashSet(StringComparer.OrdinalIgnoreCase);
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
                var (identInfo, pkColumns) = await sqlConnection.QueryAsync<(string Name, long Increment, long First), string>($"""
                    SELECT name, CONVERT(bigint, increment_value),
                        ISNULL(CONVERT(bigint, last_value) + CONVERT(bigint, increment_value), CONVERT(bigint, seed_value))
                        FROM sys.identity_columns WHERE object_id = OBJECT_ID(N'{source.Name}');

                    SELECT c.name
                        FROM sys.indexes i
                        INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
                        WHERE i.object_id = OBJECT_ID(N'{source.Name}') AND i.is_primary_key = 1
                        ORDER BY ic.key_ordinal;
                    """, transaction);

                var ident = identInfo.FirstOrDefault();
                if (ident.Name != null && !fieldSet.Contains(ident.Name))
                {
                    long start = ident.First;
                    long incr = ident.Increment;
                    generateLastColumn = i => start + (long)i * incr;
                    fieldList.Add(ident.Name);
                    fieldSet.Add(ident.Name);
                }

                var pkList = pkColumns.ToList();
                if (pkList.Count == 1 && fieldSet.Contains(pkList[0]))
                {
                    captureColumnIndex = fieldList.FindIndex(f => string.Equals(f, pkList[0], StringComparison.OrdinalIgnoreCase));
                    capturedKeys = [];
                }
            }

            await using var reader = source.EnumerateDataAsync().GetAsyncEnumerator();
            using var dataSource = new DataReader(fieldList, reader, ResolveReference, captureColumnIndex, capturedKeys, generateLastColumn);
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
                if (generateLastColumn != null)
                {
                    long firstId = (long)generateLastColumn(0);
                    long lastId = (long)generateLastColumn(dataSource.RowCount - 1);
                    insertedIdRanges.Add(new InsertedIdRange(source.Name, firstId, lastId));
                }

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
