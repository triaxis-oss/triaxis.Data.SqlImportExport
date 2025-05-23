
namespace triaxis.Data.SqlImportExport;

public interface IBulkImportSource
{
    string Name { get; }

    Task<IEnumerable<string>> GetColumnNamesAsync();
    IAsyncEnumerable<object[]> EnumerateDataAsync();
}
