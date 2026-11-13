using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Workshop.EventStore;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlServerEventStore(this IServiceCollection services, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        services.AddDbContextFactory<EventStoreDbContext>(options => options.UseSqlServer(connectionString));
        services.AddScoped<IEventStore, EfEventStore>();
        return services;
    }
}
