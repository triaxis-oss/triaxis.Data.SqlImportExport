namespace triaxis.Data.SqlImportExport;

public class BulkImportOptions
{
    public static TimeSpan DefaultTimeout => TimeSpan.FromMinutes(5);
    public const int DefaultBatchSize = 1000;

    public TimeSpan? Timeout { get; init; }
    public int? BatchSize { get; init; }
    public bool Truncate { get; init; }
    public bool SkipIdentity { get; init; }
    public bool KeepNulls { get; init; }
}
