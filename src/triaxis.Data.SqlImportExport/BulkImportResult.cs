namespace triaxis.Data.SqlImportExport;

public class BulkImportResult
{
    public static readonly BulkImportResult Empty = new(new Dictionary<string, InsertedIdRange>());

    internal BulkImportResult(Dictionary<string, InsertedIdRange> insertedIdRanges)
    {
        InsertedIdRanges = insertedIdRanges;
    }

    public IReadOnlyDictionary<string, InsertedIdRange> InsertedIdRanges { get; }
}
