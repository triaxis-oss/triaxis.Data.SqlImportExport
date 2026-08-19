using System.Data;
using System.Data.Common;
using System.Text;

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
        var indexStrategy = options?.IndexStrategy ?? BulkImportIndexStrategy.Maintain;
        int timeout = (int)(options?.Timeout ?? BulkImportOptions.DefaultTimeout).TotalSeconds;
        string grantCap = options?.MaxGrantPercent is int pct ? $"OPTION (MAX_GRANT_PERCENT = {pct})" : "";

        await using var transaction = await sqlConnection.BeginTransactionAsync();

        var schema = await SchemaMetadata.LoadAsync(sqlConnection, transaction);

        // KeepIdentity stays on for every source. When the source supplies the
        // identity column, those values are preserved. When it doesn't, we
        // synthesize them in DataReader using the tracked seed + increment, so
        // SqlBulkCopy still sees a value (SET IDENTITY_INSERT requires it).
        // Two writers differing in KeepNulls carry the two meanings a DBNull on the wire can
        // have. Without it the server substitutes the column default where one exists - exactly
        // what an omitted (null) cell asks for - so omitted cells ride as DBNull through the
        // default writer instead of forcing columns out of the mapping. With it a DBNull is a
        // literal NULL even where the column has a default, which is the one thing the default
        // writer cannot say and the only reason a row ever needs the keepNulls one.
        SqlBulkCopy CreateWriter(SqlBulkCopyOptions options) => new(sqlConnection, options, (SqlTransaction)transaction)
        {
            BulkCopyTimeout = timeout,
            EnableStreaming = true,
            BatchSize = batchSize,
        };
        using var bcpKeepNulls = CreateWriter(SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls);
        using var bcpDefaults = CreateWriter(SqlBulkCopyOptions.KeepIdentity);

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

            var fieldList = (await source.GetColumnNamesAsync()).ToList();
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

            var defaultColumns = new bool[fieldList.Count];
            var notNullColumns = new bool[fieldList.Count];
            for (int i = 0; i < fieldList.Count; i++)
            {
                defaultColumns[i] = table.DefaultColumns.Contains(fieldList[i]);
                notNullColumns[i] = table.NotNullColumns.Contains(fieldList[i]);
            }

            await using var reader = source.EnumerateDataAsync().GetAsyncEnumerator();
            using var dataSource = new DataReader(fieldList, reader, ResolveReference, captureColumnIndex, capturedKeys, generateLastColumn, defaultColumns, notNullColumns);

            if (merge)
            {
                // Merging needs somewhere to put the incoming rows before matching them up. All of
                // them land there in a single write with every column mapped, and the shape column
                // records which columns each row actually supplied, so the merge can handle every
                // shape with its own column list: inserts let the table fill in defaults for the
                // omitted columns, updates leave them untouched.
                await CreateStagingTableAsync(sqlConnection, transaction, source.Name, fieldList, notNullColumns, timeout);
                dataSource.EnterStagingMode();
                bcpKeepNulls.DestinationTableName = $"#{source.Name}";
                MapStagingColumns(bcpKeepNulls, dataSource);
                await bcpKeepNulls.WriteToServerAsync(dataSource);
                await sqlConnection.ExecuteAsync(
                    MergeSql(source.Name, fieldList, dataSource.Shapes, strategy, table, grantCap), transaction, timeout);
            }
            else
            {
                async Task WriteRunAsync()
                {
                    var bcp = dataSource.RunKeepNulls ? bcpKeepNulls : bcpDefaults;
                    bcp.DestinationTableName = source.Name;
                    bcp.ColumnMappings.Clear();
                    bcp.ColumnOrderHints.Clear();

                    for (int i = 0; i < dataSource.Fields.Length; i++)
                    {
                        if (dataSource.RunSupplies(i))
                        {
                            bcp.ColumnMappings.Add(i, dataSource.Fields[i]);
                        }
                    }

                    if (bcp.ColumnMappings.Count == 0)
                    {
                        // empty mappings would make SqlBulkCopy map every column by ordinal, turning
                        // "nothing supplied" into a row of NULLs - rows shaped like that insert their
                        // defaults instead
                        int emptyRows = 0;
                        while (dataSource.Read()) emptyRows++;
                        await sqlConnection.ExecuteAsync($"""
                            DECLARE @n int = {emptyRows};
                            WHILE @n > 0 BEGIN INSERT INTO {source.Name} DEFAULT VALUES; SET @n -= 1; END;
                            """, transaction, timeout);
                        return;
                    }

                    // The only thing SqlBulkCopy ever tells the server about the incoming rows is how they
                    // are sorted - it never sends a row count - so this is the one chance to spare it the
                    // clustered index sort and the memory grant that comes with it. The source knows its
                    // own order; failing that, we know the order of identity values we generated ourselves.
                    foreach (var hint in source.SortedBy)
                    {
                        int hintIndex = dataSource.GetOrdinal(hint.Column);
                        if (hintIndex >= 0 && !dataSource.RunSupplies(hintIndex))
                        {
                            // a hint prefix stays valid for any subsequence of the rows; from the
                            // first unmapped column on, the remainder alone would claim an order
                            // the rows were never sorted by
                            break;
                        }
                        bcp.ColumnOrderHints.Add(hint);
                    }

                    if (bcp.ColumnOrderHints.Count == 0
                        && generateLastColumn != null && table.IdentityIncrement > 0 && !table.ClusteredKeyDescending
                        && table.ClusteredKeyColumns is [var clusteredKey]
                        && string.Equals(clusteredKey, table.IdentityColumn, StringComparison.OrdinalIgnoreCase))
                    {
                        bcp.ColumnOrderHints.Add(clusteredKey, SortOrder.Ascending);
                    }

                    await bcp.WriteToServerAsync(dataSource);
                }

                // Rows group into write classes (see DataReader.FindOrAddClass) - most jagged
                // sources collapse into a single one. Buffering a bounded prefix decides how to
                // write without a second pass over the source: a source that fits the buffer
                // entirely - seed data - costs one bulk copy per class however its rows
                // interleave, while a single-class prefix that outgrows the buffer - the big
                // dataset - keeps one write open and streams the rest through it until the class
                // changes, never holding more than the buffered rows. Only a big source that
                // keeps switching classes pays per chunk.
                int bufferRows = options?.MaxBufferedRows ?? BulkImportOptions.DefaultMaxBufferedRows;
                while (await dataSource.FillBufferAsync(bufferRows))
                {
                    if (!dataSource.SourceExhausted && dataSource.BufferedClassCount == 1)
                    {
                        dataSource.BeginStreamingRun();
                        await WriteRunAsync();
                    }
                    else
                    {
                        while (dataSource.NextFlushClass())
                        {
                            await WriteRunAsync();
                        }
                    }
                }
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
                        // a null key cell left the value for the server to assign, so the import
                        // never saw it - resolving to it must fail rather than hand out a NULL
                        var keys = capturedKeys;
                        string name = source.Name;
                        sourceReferenceMap[source] = i => keys[i] is not DBNull ? keys[i]
                            : throw new InvalidOperationException(
                                $"Cannot resolve reference to row {i} of source '{name}' - its key was left for the server to assign.");
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
    /// Creates a tempdb buffer for the columns the source supplies, plus the shape id of every
    /// row. NOT NULL columns (the identity included) are wrapped in NULLIF to shed the
    /// constraint and the IDENTITY property on the way (no data flows through it - TOP 0 only
    /// carries the type): the buffer has to store whatever the rows carry, including NULL where
    /// a row omitted the cell, and it is the statements moving the rows out that face the real
    /// table's rules. Nullable columns pass through verbatim, keeping non-comparable types
    /// (xml, geography, ...) usable - NULLIF needs '=' - at the price of a NOT NULL
    /// non-comparable column staying unsupported in staged paths.
    /// </summary>
    private static Task CreateStagingTableAsync(SqlConnection sqlConnection, DbTransaction transaction, string table, List<string> fields, bool[] notNullColumns, int timeout)
        => sqlConnection.ExecuteAsync($"""
            IF OBJECT_ID('tempdb..#{table}') IS NOT NULL DROP TABLE [#{table}];
            SELECT TOP 0 {string.Join(", ", fields.Select((f, i) => notNullColumns[i] ? $"[{f}] = NULLIF([{f}], [{f}])" : $"[{f}]"))},
                [{DataReader.ShapeField}] = CONVERT(int, 0)
                INTO [#{table}] FROM {table};
            """, transaction, timeout);

    private static void MapStagingColumns(SqlBulkCopy bcp, DataReader dataSource)
    {
        bcp.ColumnMappings.Clear();
        bcp.ColumnOrderHints.Clear();
        for (int i = 0; i < dataSource.Fields.Length; i++)
        {
            bcp.ColumnMappings.Add(i, dataSource.Fields[i]);
        }
        bcp.ColumnMappings.Add(dataSource.Fields.Length, DataReader.ShapeField);
    }

    private static IEnumerable<string> ShapeColumns(List<string> fields, bool[] shape)
        => fields.Where((_, i) => shape[i]);

    private static bool NeedsIdentityInsert(IEnumerable<string> columns, TableMetadata metadata)
        => metadata.IdentityColumn != null && columns.Contains(metadata.IdentityColumn, StringComparer.OrdinalIgnoreCase);

    private static string MergeSql(string table, List<string> fields, IReadOnlyList<bool[]> shapes, BulkImportStrategy strategy, TableMetadata metadata, string grantCap)
    {
        // Only indexes whose key columns the source supplies in full can match rows up - the
        // staging table carries nothing else to compare, and rows matching a key they did not
        // supply would be an accident; with no such index, nothing can match and all rows
        // insert. INCLUDE columns are not part of the key and must not disqualify an index,
        // and the name comparison pins a case-insensitive collation so it agrees with the
        // client-side OrdinalIgnoreCase matching even on a case-sensitive database. Duplicate
        // keys arriving under different shapes resolve in shape order, not source order.
        string suppliedList = string.Join(", ", fields.Select(f => $"N'{f.Replace("'", "''")}'"));
        var sb = new StringBuilder($"""
            DECLARE @condition NVARCHAR(max), @sql NVARCHAR(max);
            SELECT @condition = CONCAT('(', STRING_AGG(s, ') OR ('), ')')
                FROM (select STRING_AGG(CONCAT('s.[', c.name, '] = t.[', c.name, ']'), ' AND ') s from sys.columns c
                INNER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id AND ic.is_included_column = 0
                INNER JOIN sys.indexes ix ON ix.object_id = c.object_id AND ic.index_id = ix.index_id
                WHERE c.object_id = OBJECT_ID('{table}')
                GROUP BY ix.index_id
                HAVING COUNT(CASE WHEN c.name COLLATE Latin1_General_100_CI_AS NOT IN ({suppliedList}) THEN 1 END) = 0) x;
            SET @condition = ISNULL(@condition, '1 = 0');
            {(shapes.Count > 1 ? $"CREATE CLUSTERED INDEX [IX_shape] ON [#{table}] ([{DataReader.ShapeField}]);" : "")}

            """);

        for (int s = 0; s < shapes.Count; s++)
        {
            var cols = ShapeColumns(fields, shapes[s]).ToList();
            bool identityInsert = NeedsIdentityInsert(cols, metadata);
            string insert = cols.Count > 0
                ? $"INSERT ({string.Join(", ", cols.Select(f => $"[{f}]"))}) VALUES ({string.Join(", ", cols.Select(f => $"s.[{f}]"))})"
                : "INSERT DEFAULT VALUES";
            string updateSet = string.Join(", ", cols
                .Where(f => !string.Equals(f, metadata.IdentityColumn, StringComparison.OrdinalIgnoreCase))
                .Select(f => $"t.[{f}] = s.[{f}]"));

            sb.AppendLine($"""
                {(identityInsert ? $"SET IDENTITY_INSERT [{table}] ON;" : "")}
                SET @sql = CONCAT(N'MERGE INTO {table} t USING (SELECT * FROM [#{table}] WHERE [{DataReader.ShapeField}] = {s}) s ON (', @condition, ')
                    WHEN NOT MATCHED THEN {insert}
                    {(strategy == BulkImportStrategy.Upsert && updateSet.Length > 0 ? $"WHEN MATCHED THEN UPDATE SET {updateSet}" : "")}
                    {grantCap};
                    ');
                EXEC sp_executesql @sql;
                {(identityInsert ? $"SET IDENTITY_INSERT [{table}] OFF;" : "")}
                """);
        }

        sb.AppendLine($"DROP TABLE [#{table}];");
        return sb.ToString();
    }

    private class DataReader : IDataReader
    {
        /// <summary>Extra staging column carrying each row's shape id.</summary>
        public const string ShapeField = "__$shape";

        private readonly string[] _fields;
        private readonly Func<BulkImportSourceReference, object>? _resolveReference;
        private readonly int _captureColumnIndex;
        private readonly List<object>? _captureTarget;
        private readonly Func<int, object>? _generateLastColumn;
        private readonly bool[] _defaultColumns;
        private readonly bool[] _notNullColumns;
        private IAsyncEnumerator<object?[]>? _data;
        private object?[] _values = null!;
        private object?[]? _pendingRow;
        private readonly List<bool[]> _shapes = [];
        private readonly List<(bool KeepNulls, bool[] Mapped)> _classes = [];
        private readonly List<List<object?[]>> _buffers = [];
        private object _shapeValue = null!;
        private bool _staging;
        private bool _continueLive;
        private int _runClass = -1;
        private int _replayClass = -1;
        private int _replayPos;
        private object _syntheticValue = null!;

        public DataReader(IEnumerable<string> fields, IAsyncEnumerator<object?[]> data, Func<BulkImportSourceReference, object>? resolveReference = null, int captureColumnIndex = -1, List<object>? captureTarget = null, Func<int, object>? generateLastColumn = null, bool[]? defaultColumns = null, bool[]? notNullColumns = null)
        {
            _fields = fields.ToArray();
            _data = data;
            _resolveReference = resolveReference;
            _captureColumnIndex = captureColumnIndex;
            _captureTarget = captureTarget;
            _generateLastColumn = generateLastColumn;
            _defaultColumns = defaultColumns ?? new bool[_fields.Length];
            _notNullColumns = notNullColumns ?? new bool[_fields.Length];
        }

        public object this[int i] =>
            i < _values.Length ? _values[i] ?? DBNull.Value :
            i < _fields.Length ? _syntheticValue :
            _shapeValue;

        public object this[string name] => throw new NotImplementedException();

        public int Depth => 0;
        public bool IsClosed => _data == null;
        public int RecordsAffected => 0;
        public int FieldCount => _fields.Length + (_staging ? 1 : 0);
        public string[] Fields => _fields;
        public int RowCount { get; private set; }
        /// <summary>Distinct row shapes seen so far, as supplied-column masks indexed by shape id.</summary>
        public IReadOnlyList<bool[]> Shapes => _shapes;

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
            for (int i = 0; i < FieldCount; i++)
            {
                values[i] = this[i];
            }
            return FieldCount;
        }

        public bool IsDBNull(int i) => this[i] == DBNull.Value;

        public string GetName(int i) => i < _fields.Length ? _fields[i] : ShapeField;
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

        /// <summary>Nothing left to read - the source is drained and no row is held back.</summary>
        public bool SourceExhausted => _data == null && _pendingRow == null;

        /// <summary>Distinct write classes with rows in the current buffer.</summary>
        public int BufferedClassCount => _buffers.Count(b => b.Count > 0);

        /// <summary>
        /// Buffers up to <paramref name="limit"/> rows (at least one) in memory, grouped by
        /// write class. Row processing - references, key capture, identity synthesis - happens
        /// here, in source order, so however the buffered rows are regrouped on the way out, the
        /// values stay those of a straight streamed write. False when the source has nothing more.
        /// </summary>
        public async ValueTask<bool> FillBufferAsync(int limit)
        {
            foreach (var buffer in _buffers)
            {
                buffer.Clear();
            }
            _replayClass = -1;
            _continueLive = false;

            int target = Math.Max(1, limit);
            int count = 0;
            var row = _pendingRow;
            _pendingRow = null;
            while (true)
            {
                if (row is null)
                {
                    if (_data is null || !await _data.MoveNextAsync())
                    {
                        Close();
                        break;
                    }
                    row = _data.Current;
                }

                ProcessRow(row);
                int c = FindOrAddClass(row);
                while (_buffers.Count <= c)
                {
                    _buffers.Add([]);
                }
                // the buffer outlives the enumerator's next MoveNext, and a source may legally
                // reuse one row array across yields - always copy what is retained, materializing
                // the synthesized identity on the way
                var stored = new object?[_fields.Length];
                row.CopyTo(stored, 0);
                if (_generateLastColumn != null)
                {
                    stored[^1] = _syntheticValue;
                }
                _buffers[c].Add(stored);

                if (++count >= target)
                {
                    break;
                }
                row = null;
            }
            return count > 0;
        }

        /// <summary>
        /// Starts a run serving the single buffered class, then keeps streaming live rows of the
        /// same class through it until the class changes or the source ends.
        /// </summary>
        public void BeginStreamingRun()
        {
            _replayClass = _buffers.FindIndex(b => b.Count > 0);
            _replayPos = 0;
            _continueLive = true;
            _runClass = _replayClass;
        }

        /// <summary>Moves to the next buffered class with rows to flush; false when none remain.</summary>
        public bool NextFlushClass()
        {
            _continueLive = false;
            while (++_replayClass < _buffers.Count)
            {
                if (_buffers[_replayClass].Count > 0)
                {
                    _replayPos = 0;
                    _runClass = _replayClass;
                    return true;
                }
            }
            return false;
        }

        /// <summary>Whether the current run's write maps the column.</summary>
        public bool RunSupplies(int i) => _classes[_runClass].Mapped[i];

        /// <summary>Whether the current run needs KeepNulls - some cell is a literal NULL in a column with a default.</summary>
        public bool RunKeepNulls => _classes[_runClass].KeepNulls;

        /// <summary>
        /// Stops splitting rows into same-shape runs: every remaining row goes out with all
        /// columns mapped plus <see cref="ShapeField"/> carrying its shape id, so the SQL moving
        /// the rows onward can still treat each shape separately.
        /// </summary>
        public void EnterStagingMode() => _staging = true;

        // Cells past the row's end are the synthetic identity column, which is always supplied.
        // A DBNull aimed at a NOT NULL column that has a default also counts as omitted: NULL is
        // not storable there, the default is the only meaningful outcome, and CSV can express
        // nothing else for a missing field.
        private bool Omitted(object?[] row, int i)
            => i < row.Length && (row[i] is null || (row[i] is DBNull && _notNullColumns[i] && _defaultColumns[i]));

        private bool Supplies(object?[] row, int i) => !Omitted(row, i);

        // A write class is what one bulk copy can carry: a mapping plus the KeepNulls flag.
        // Without KeepNulls a mapped DBNull already means "column default where one exists, NULL
        // otherwise" - the exact meaning of an omitted cell - so omitted cells stay mapped and
        // most rows share a single class no matter which columns they carry. Only a literal NULL
        // in a column with a storable default forces KeepNulls, and such rows must then drop the
        // columns they omit (among those with defaults) from the mapping instead. An omitted NOT
        // NULL column (the identity included) always leaves the mapping: SqlBulkCopy rejects a
        // DBNull against it client-side, before the server could substitute anything.
        private bool NeedsKeepNulls(object?[] row)
        {
            for (int i = 0; i < row.Length; i++)
            {
                if (_defaultColumns[i] && !_notNullColumns[i] && row[i] == DBNull.Value)
                {
                    return true;
                }
            }
            return false;
        }

        private bool Unmapped(object?[] row, bool keepNulls, int i)
            => Omitted(row, i) && (_notNullColumns[i] || (keepNulls && _defaultColumns[i]));

        private bool MatchesClass(object?[] row, int cls)
        {
            var (keepNulls, mapped) = _classes[cls];
            if (NeedsKeepNulls(row) != keepNulls)
            {
                return false;
            }
            for (int i = 0; i < mapped.Length; i++)
            {
                if (Unmapped(row, keepNulls, i) == mapped[i])
                {
                    return false;
                }
            }
            return true;
        }

        private int FindOrAddClass(object?[] row)
        {
            for (int c = 0; c < _classes.Count; c++)
            {
                if (MatchesClass(row, c))
                {
                    return c;
                }
            }

            bool keepNulls = NeedsKeepNulls(row);
            var mapped = new bool[_fields.Length];
            for (int i = 0; i < mapped.Length; i++)
            {
                mapped[i] = !Unmapped(row, keepNulls, i);
            }
            _classes.Add((keepNulls, mapped));
            return _classes.Count - 1;
        }

        private bool MatchesShape(object?[] row, bool[] mask)
        {
            for (int i = 0; i < mask.Length; i++)
            {
                if (Supplies(row, i) != mask[i])
                {
                    return false;
                }
            }
            return true;
        }

        private int FindOrAddShape(object?[] row)
        {
            for (int s = 0; s < _shapes.Count; s++)
            {
                if (MatchesShape(row, _shapes[s]))
                {
                    return s;
                }
            }

            var mask = new bool[_fields.Length];
            for (int i = 0; i < mask.Length; i++)
            {
                mask[i] = Supplies(row, i);
            }
            _shapes.Add(mask);
            return _shapes.Count - 1;
        }

        public bool Read()
        {
            if (_replayClass >= 0)
            {
                // buffered rows were fully processed on the way in - just serve the current class
                var buffer = _buffers[_replayClass];
                if (_replayPos < buffer.Count)
                {
                    _values = buffer[_replayPos++];
                    return true;
                }
                if (!_continueLive)
                {
                    return false;
                }
                // buffered prefix served - continue with live rows of the same class
                _replayClass = -1;
            }

            object?[]? row = _pendingRow;
            if (row is not null)
            {
                _pendingRow = null;
            }
            else
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

                row = _data.Current;
                if (!_staging && !MatchesClass(row, _runClass))
                {
                    // a row of a different class ends this bulk copy; FillBufferAsync picks it up
                    _pendingRow = row;
                    return false;
                }
            }

            if (_staging)
            {
                _shapeValue = FindOrAddShape(row);
            }

            ProcessRow(row);
            return true;
        }

        private void ProcessRow(object?[] row)
        {
            _values = row;
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
        }
    }
}
