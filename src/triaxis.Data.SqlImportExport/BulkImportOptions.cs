namespace triaxis.Data.SqlImportExport;

public class BulkImportOptions
{
    public static TimeSpan DefaultTimeout => TimeSpan.FromMinutes(5);
    /// <summary>
    /// Rows per bulk copy batch. Zero sends every row in a single batch.
    /// </summary>
    /// <remarks>
    /// The knob is round-trip bound, not memory bound: streaming is enabled, so rows are never
    /// buffered client side, and the whole import already runs in one transaction, so batching
    /// buys no durability - every batch simply costs another round trip. Loading 164k rows
    /// measured 2,208ms at this default, 1,157ms at 10,000, and fastest as a single batch, so
    /// raise it, or set it to zero, when the import is large and the connection is not.
    /// </remarks>
    public const int DefaultBatchSize = 1000;

    public TimeSpan? Timeout { get; init; }
    public int? BatchSize { get; init; }
    public BulkImportStrategy Strategy { get; init; }
    public BulkImportIndexStrategy IndexStrategy { get; init; }
    /// <summary>
    /// Caps the memory grant the <c>MERGE</c> behind <see cref="BulkImportStrategy.Upsert"/> and
    /// <see cref="BulkImportStrategy.InsertIgnore"/> may ask for, as a percentage of what the
    /// server will hand to one query. Null leaves it uncapped.
    /// </summary>
    /// <remarks>
    /// Only that path takes a grant worth capping - a bulk insert asks blind, but a MERGE out of
    /// the staging table plans against the real row count and sizes its sorts accordingly, which
    /// on a memory-constrained server is what queues on RESOURCE_SEMAPHORE. Capping too hard
    /// spills the sort to tempdb rather than failing, so this trades a hard stop for a slower sort.
    /// </remarks>
    public int? MaxGrantPercent { get; init; }
    public bool KeepNulls { get; init; }
    public bool DryRun { get; init; }
    public bool SkipConstraints { get; init; }
}
