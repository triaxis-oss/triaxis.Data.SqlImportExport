namespace triaxis.Data.SqlImportExport;

public static class ExportExtensions
{
    public static async Task ToCsvDirectoryAsync(this IAsyncEnumerable<IBulkExportTable> export, string outputDirectory, bool includeEmptyTables = false, Func<object?, string>? formatter = null)
    {
        await foreach (var table in export)
        {
            CsvWriter? writer = null;

            CsvWriter RequireWriter() => writer ??= CreateWriter();
            CsvWriter CreateWriter()
            {
                var fileName = Path.Combine(outputDirectory, table.Name + ".csv");
                Directory.CreateDirectory(outputDirectory);
                return new CsvWriter(fileName, table.Columns.Select(c => c.Name), formatter);
            }

            try
            {
                await foreach (var row in table.GetRowsAsync())
                {
                    await RequireWriter().WriteRecordAsync(row);
                }

                if (includeEmptyTables)
                {
                    RequireWriter();
                }
            }
            finally
            {
                if (writer is not null)
                {
                    await writer.DisposeAsync();
                }
            }
        }
    }
}
