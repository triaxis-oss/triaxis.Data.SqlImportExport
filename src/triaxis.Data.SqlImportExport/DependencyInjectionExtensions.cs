using triaxis.Data.SqlImportExport;

namespace Microsoft.Extensions.DependencyInjection;

public static class DependencyInjectionExtensions
{
    public static IServiceCollection AddSqlImportExport(this IServiceCollection services)
    {
        services.AddTransient<IBulkExportService, BulkExportService>();
        services.AddTransient<IBulkImportService, BulkImportService>();
        return services;
    }
}
