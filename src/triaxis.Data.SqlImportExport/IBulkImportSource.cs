
namespace triaxis.Data.SqlImportExport;

public interface IBulkImportSource
{
    string Name { get; }
    /// <summary>
    /// Override the import strategy for this source. If null, the strategy from BulkImportOptions is used.
    /// </summary>
    BulkImportStrategy? Strategy => null;

    Task<IEnumerable<string>> GetColumnNamesAsync();
    IAsyncEnumerable<object[]> EnumerateDataAsync();
}
