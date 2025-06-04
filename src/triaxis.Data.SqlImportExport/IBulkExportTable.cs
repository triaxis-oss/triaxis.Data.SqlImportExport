namespace triaxis.Data.SqlImportExport;

public interface IBulkExportTable
{
    string Name { get; }
    IReadOnlyList<IBulkExportColumn> Columns { get; }
    IAsyncEnumerable<IReadOnlyList<object?>> GetRowsAsync();
}
