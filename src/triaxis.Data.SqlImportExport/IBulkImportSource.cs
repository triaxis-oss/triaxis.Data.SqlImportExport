
namespace triaxis.Data.SqlImportExport;

public interface IBulkImportSource
{
    string Name { get; }
    bool? Truncate => null;

    Task<IEnumerable<string>> GetColumnNamesAsync();
    IAsyncEnumerable<object[]> EnumerateDataAsync();
}
