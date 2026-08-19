using System.Data.Common;

namespace triaxis.Data.SqlImportExport;

/// <summary>
/// Identity, primary key, index and default-constraint metadata for every table in the
/// database, fetched in a single round trip.
/// </summary>
/// <remarks>
/// Asking per table is what this replaces: the per-table variant interpolates the table name
/// into the query text, so every table compiles its own plan against the system views, which
/// measured ~29ms per table - more than the bulk copy it precedes. The same query parameterised
/// costs 1.3ms; covering every table in one go costs 0.2ms each.
/// </remarks>
internal sealed class SchemaMetadata
{
    private readonly Dictionary<string, TableMetadata> _tables = new(StringComparer.OrdinalIgnoreCase);

    public static async Task<SchemaMetadata> LoadAsync(SqlConnection connection, DbTransaction? transaction)
    {
        var (columns, indexes) = await connection.QueryAsync<ColumnRow, IndexRow>("""
            SELECT OBJECT_SCHEMA_NAME(c.object_id), OBJECT_NAME(c.object_id), c.name, c.is_identity,
                CONVERT(bigint, ic.increment_value), CONVERT(bigint, ic.seed_value),
                ISNULL(CONVERT(bigint, ic.last_value) + CONVERT(bigint, ic.increment_value), CONVERT(bigint, ic.seed_value)),
                CONVERT(bit, IIF(c.default_object_id <> 0, 1, 0)), CONVERT(bit, IIF(c.is_nullable = 0, 1, 0))
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE c.is_identity = 1 OR c.default_object_id <> 0 OR c.is_nullable = 0;

            SELECT OBJECT_SCHEMA_NAME(i.object_id), OBJECT_NAME(i.object_id), CONVERT(bit, i.is_primary_key), c.name, i.name,
                CONVERT(bit, IIF(i.index_id = 1, 1, 0)), CONVERT(bit, ic.is_descending_key),
                -- a plain enabled nonclustered index is one we may disable for the load; anything
                -- backing a key or pointed at by a foreign key has to stay where it is
                CONVERT(bit, IIF(i.type = 2 AND i.is_disabled = 0 AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
                    AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys fk
                        WHERE fk.referenced_object_id = i.object_id AND fk.key_index_id = i.index_id), 1, 0))
                FROM sys.indexes i
                INNER JOIN sys.tables t ON t.object_id = i.object_id
                INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
                WHERE i.index_id > 0
                ORDER BY i.object_id, i.index_id, ic.key_ordinal;
            """, transaction);

        var result = new SchemaMetadata();

        foreach (var c in columns)
        {
            var table = result.GetOrAdd(c.Schema, c.Table);
            if (c.IsIdentity)
            {
                table.IdentityColumn = c.Column;
                table.IdentityIncrement = c.Increment ?? 1;
                table.IdentitySeed = c.Seed ?? 1;
                table.NextIdentity = c.Next;
            }
            if (c.HasDefault)
            {
                table.DefaultColumns.Add(c.Column);
            }
            if (c.NotNull)
            {
                table.NotNullColumns.Add(c.Column);
            }
        }

        foreach (var i in indexes)
        {
            var table = result.GetOrAdd(i.Schema, i.Table);
            if (i.IsPrimaryKey)
            {
                table.PrimaryKeyColumns.Add(i.Column);
            }
            if (i.IsClustered)
            {
                table.ClusteredKeyColumns.Add(i.Column);
                if (i.IsDescending)
                {
                    table.ClusteredKeyDescending = true;
                }
            }
            // the query returns one row per index column, so the same index arrives repeatedly
            if (i.CanDisable && !table.SecondaryIndexes.Contains(i.Index))
            {
                table.SecondaryIndexes.Add(i.Index);
            }
        }

        return result;
    }

    /// <summary>
    /// Metadata for a table the import targets. Tables with no identity column, no defaults, no
    /// NOT NULL columns and no indexes never show up in either query, so an absent entry is a
    /// valid empty one.
    /// </summary>
    public TableMetadata this[string name]
    {
        get
        {
            if (!_tables.TryGetValue(Unquote(name), out var table))
            {
                _tables[Unquote(name)] = table = new TableMetadata();
            }
            return table;
        }
    }

    private TableMetadata GetOrAdd(string schema, string name)
    {
        if (!_tables.TryGetValue(name, out var table))
        {
            _tables[name] = table = new TableMetadata();
        }
        // qualified name resolves to the same instance, so callers may use either form
        _tables[$"{schema}.{name}"] = table;
        return table;
    }

    private static string Unquote(string name) => name.Replace("[", "").Replace("]", "");

    private record ColumnRow(string Schema, string Table, string Column, bool IsIdentity, long? Increment, long? Seed, long? Next, bool HasDefault, bool NotNull);
    private record IndexRow(string Schema, string Table, bool IsPrimaryKey, string Column, string Index, bool IsClustered, bool IsDescending, bool CanDisable);
}

internal sealed class TableMetadata
{
    public string? IdentityColumn { get; set; }
    public long IdentityIncrement { get; set; } = 1;
    public long IdentitySeed { get; set; } = 1;
    public List<string> PrimaryKeyColumns { get; } = [];
    /// <summary>Key columns of the clustered index, in key order; empty for a heap.</summary>
    public List<string> ClusteredKeyColumns { get; } = [];
    public bool ClusteredKeyDescending { get; set; }
    /// <summary>Nonclustered indexes that may be disabled for the duration of a load.</summary>
    public List<string> SecondaryIndexes { get; } = [];
    public HashSet<string> DefaultColumns { get; } = new(StringComparer.OrdinalIgnoreCase);
    public HashSet<string> NotNullColumns { get; } = new(StringComparer.OrdinalIgnoreCase);
    public bool IndexesDisabled { get; set; }

    /// <summary>
    /// Next identity value to synthesize, or null when the seed is no longer known and has to be
    /// re-read - anything that inserts values we didn't pick ourselves invalidates it.
    /// </summary>
    public long? NextIdentity { get; set; }

    public void ResetIdentity() => NextIdentity = IdentitySeed;

    public async Task<long> GetNextIdentityAsync(SqlConnection connection, DbTransaction? transaction, string tableName)
    {
        if (NextIdentity is not long next)
        {
            NextIdentity = next = (await connection.QueryAsync<long>("""
                SELECT ISNULL(CONVERT(bigint, last_value) + CONVERT(bigint, increment_value), CONVERT(bigint, seed_value))
                    FROM sys.identity_columns WHERE object_id = OBJECT_ID(@table);
                """, transaction, ("@table", tableName))).First();
        }
        return next;
    }
}
