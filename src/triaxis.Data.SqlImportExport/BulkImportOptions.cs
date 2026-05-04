namespace triaxis.Data.SqlImportExport;

public class BulkImportOptions
{
    public static TimeSpan DefaultTimeout => TimeSpan.FromMinutes(5);
    public const int DefaultBatchSize = 1000;

    public TimeSpan? Timeout { get; init; }
    public int? BatchSize { get; init; }
    public BulkImportStrategy Strategy { get; init; }
    public bool KeepNulls { get; init; }
    public bool DryRun { get; init; }
    public bool SkipConstraints { get; init; }
}
