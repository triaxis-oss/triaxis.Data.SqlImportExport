namespace triaxis.Data.SqlImportExport;

public interface IBulkExportColumn
{
    string Name { get; }
    Type Type { get; }
    object? DefaultValue { get; }
}
