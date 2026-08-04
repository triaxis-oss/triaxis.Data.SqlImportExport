namespace triaxis.Data.SqlImportExport;

/// <summary>
/// Controls what happens to secondary indexes on the destination during the load.
/// </summary>
/// <remarks>
/// A bulk insert into an indexed table compiles one Sort per index, and the memory grant is sized
/// to sort the whole input - tens of megabytes against a stated requirement of half of one, not
/// scaling with the number of rows. On a memory-constrained server those requests can queue on
/// RESOURCE_SEMAPHORE. Better cardinality does not help; only removing the sorts does.
/// </remarks>
public enum BulkImportIndexStrategy
{
    /// <summary>
    /// Leave indexes in place and let the bulk insert maintain them. Fastest when the server has
    /// memory to spare, and what every version before this one did.
    /// </summary>
    Maintain = 0,
    /// <summary>
    /// Disable secondary indexes before loading a table and rebuild them once the load is done.
    /// The load itself then compiles no sorts at all, and the rebuild that follows plans against
    /// a row count the server actually knows.
    /// </summary>
    Rebuild = 1,
}
