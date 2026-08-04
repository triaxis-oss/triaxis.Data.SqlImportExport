
namespace triaxis.Data.SqlImportExport;

public interface IBulkImportSource
{
    string Name { get; }
    /// <summary>
    /// Override the import strategy for this source. If null, the strategy from BulkImportOptions is used.
    /// </summary>
    BulkImportStrategy? Strategy => null;

    /// <summary>
    /// Order the rows are already in, when they are. A bulk insert into an indexed table otherwise
    /// compiles a Sort for the clustered index and asks for a memory grant to run it; saying the
    /// rows already arrive in that order spares it both. Declaring an order the rows are not
    /// actually in fails the import, so only say so when the source guarantees it.
    /// </summary>
    /// <remarks>
    /// Left empty, and with the service generating the identity values itself, it works the order
    /// out on its own - it picked those values, so it knows they ascend.
    /// </remarks>
    IEnumerable<SqlBulkCopyColumnOrderHint> SortedBy => [];

    /// <summary>
    /// Columns any row of this source may supply - the union, when rows differ in shape.
    /// </summary>
    Task<IEnumerable<string>> GetColumnNamesAsync();
    /// <summary>
    /// Rows, each as wide as the column list. Rows need not all carry the same columns: declare
    /// the union above and pass <see cref="DBNull.Value"/> for the ones a row does not supply,
    /// which land in the destination as the column default, or as NULL when
    /// <see cref="BulkImportOptions.KeepNulls"/> is set. There is no need to split a table into
    /// one source per distinct column set.
    /// </summary>
    IAsyncEnumerable<object[]> EnumerateDataAsync();
}
