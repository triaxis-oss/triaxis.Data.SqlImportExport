namespace triaxis.Data.SqlImportExport;

public class BulkExportOptions
{
    public Predicate<string>? TableFilter { get; init; }
    public Predicate<(string table, string column)>? ColumnFilter { get; init; }
}
