namespace triaxis.Data.SqlImportExport;

public interface IBulkExportService
{
    /// <summary>
    /// Exports data from an SQL Server database
    /// </summary>
    IAsyncEnumerable<IBulkExportTable> BulkExportAsync(SqlConnection sqlConnection, BulkExportOptions? options = null);
}
