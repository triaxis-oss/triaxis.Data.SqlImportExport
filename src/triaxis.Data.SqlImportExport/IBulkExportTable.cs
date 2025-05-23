namespace triaxis.Data.SqlImportExport;

public interface IBulkExportTable
{
    string Name { get; }
    IReadOnlyCollection<IBulkExportColumn> Columns { get; }
    IAsyncEnumerable<IEnumerable<object?>> GetRowsAsync();
}
