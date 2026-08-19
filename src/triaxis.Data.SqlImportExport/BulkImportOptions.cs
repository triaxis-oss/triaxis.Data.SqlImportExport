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

    public const int DefaultMaxBufferedRows = 8192;

    public TimeSpan? Timeout { get; init; }
    public int? BatchSize { get; init; }
    /// <summary>
    /// Rows the import may hold in memory to regroup a source whose rows cannot all share one
    /// bulk copy (see <see cref="IBulkImportSource.EnumerateDataAsync"/>), trading memory for
    /// round trips. A source fitting the buffer costs one bulk copy per distinct row group
    /// however its rows interleave; a longer uniform stretch streams straight through, the
    /// buffer only ever holding its first rows. Zero never buffers: every uniform run is
    /// written as it arrives, favoring strictly bounded memory over round trips.
    /// </summary>
    public int? MaxBufferedRows { get; init; }
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
    public bool DryRun { get; init; }
    public bool SkipConstraints { get; init; }
}
