using Microsoft.Extensions.DependencyInjection;

var builder = Tool.CreateBuilder(args);

builder.UseDefaults();

builder.ConfigureServices((_, services) =>
{
    services.AddSqlImportExport();
});

builder.Run();
