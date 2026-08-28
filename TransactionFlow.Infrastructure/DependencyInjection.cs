using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using TransactionFlow.Infrastructure.Persistence;

namespace TransactionFlow.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var connectionString =
            configuration.GetConnectionString(
                "Postgres")
            ?? throw new InvalidOperationException(
                "Postgres connection string is missing.");

        services.AddSingleton(
            NpgsqlDataSource.Create(connectionString));

        services.AddScoped<ITransactionPersistence,
            PostgresTransactionPersistence>();

        return services;
    }
}