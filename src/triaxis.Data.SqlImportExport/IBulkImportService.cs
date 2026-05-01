namespace triaxis.Data.SqlImportExport;

public interface IBulkImportService
{
    /// <summary>
    /// Imports data into an SQL Server database
    /// </summary>
    Task<BulkImportResult> BulkImportAsync(SqlConnection sqlConnection, IAsyncEnumerable<IBulkImportSource> input, BulkImportOptions? options = default);
}
