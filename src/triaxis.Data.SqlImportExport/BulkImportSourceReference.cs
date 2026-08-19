namespace triaxis.Data.SqlImportExport;

/// <summary>
/// Represents a reference to the server-generated identity value of a row from
/// a previously processed <see cref="IBulkImportSource"/>. When such a value is
/// returned from a source row, the bulk import service replaces it with the
/// actual identity value assigned to the referenced row.
/// </summary>
/// <remarks>
/// The referenced source must have already been processed by the bulk import
/// service before any source that emits a reference to it. No topological sort
/// is performed - it is the consumer's responsibility to order the sources
/// correctly. Only rows whose key the import actually saw can be referenced: a
/// value the source supplied, or an identity the service synthesized. A row
/// that left its key for the server to assign - a null identity cell - has no
/// client-side value, and resolving a reference to it throws.
/// </remarks>
public record BulkImportSourceReference(IBulkImportSource Source, int RowIndex);
