[Command("export", Description = "Export data from the database to CSV files")]
public class ExportCommand
{
    [Inject]
    private readonly IBulkExportService _export = null!;

    [Argument(Description = "The connection string to the database")]
    public required string ConnectionString { get; init; }
    [Argument(Description = "The directory to export the CSV files to")]
    public required string OutputDirectory { get; init; }

    public async Task ExecuteAsync()
    {
        await using var con = new SqlConnection(ConnectionString);

        await _export.BulkExportAsync(con).ToCsvDirectoryAsync(OutputDirectory);
    }
}
