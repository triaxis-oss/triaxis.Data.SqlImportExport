[Command("import", Description = "Import data to the database from CSV files")]
public class ImportCommand
{
    [Inject]
    private readonly IBulkImportService _import = null!;

    [Argument(Description = "The connection string to the database")]
    public required string ConnectionString { get; init; }
    [Argument(Description = "The directory to import the CSV files from")]
    public required string InputDirectory { get; init; }

    public async Task ExecuteAsync()
    {
        await using var con = new SqlConnection(ConnectionString);

        await _import.BulkImportAsync(con, CsvSource.FromDirectory(InputDirectory));
    }
}
