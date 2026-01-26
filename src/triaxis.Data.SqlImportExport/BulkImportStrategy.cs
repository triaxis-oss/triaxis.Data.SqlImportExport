namespace triaxis.Data.SqlImportExport;

public enum BulkImportStrategy
{
    /// <summary>
    /// Default BCP strategy - insert all rows, fail on duplicates. Best performance.
    /// </summary>
    Insert = 0,
    /// <summary>
    /// Merge data using temporary table and MERGE statement, replacing existing rows.
    /// </summary>
    Upsert = 1,
    /// <summary>
    /// Merge data using temporary table and MERGE statement, ignoring existing rows.
    /// </summary>
    InsertIgnore = 2,
    /// <summary>
    /// Truncate the target table before import. Doesn't work if the table has foreign key references.
    /// </summary>
    Truncate = 3,
}
