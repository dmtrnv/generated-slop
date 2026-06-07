using AbonentSource;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AbonentClient;

/// <summary>
/// DI helpers for registering <see cref="IAbonentClient"/> in a host application.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the gRPC <see cref="AbonentService.AbonentServiceClient"/> and
    /// the <see cref="IAbonentClient"/> wrapper using the default
    /// <see cref="AbonentClientOptions.SectionName"/> configuration section.
    /// </summary>
    public static IServiceCollection AddAbonentClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        return services.AddAbonentClient(configuration, AbonentClientOptions.SectionName);
    }

    /// <summary>
    /// Registers the gRPC <see cref="AbonentService.AbonentServiceClient"/> and
    /// the <see cref="IAbonentClient"/> wrapper using a custom configuration section.
    /// </summary>
    public static IServiceCollection AddAbonentClient(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);

        services
            .AddOptions<AbonentClientOptions>()
            .Bind(configuration.GetSection(sectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.Address),
                $"{nameof(AbonentClientOptions.Address)} must be configured.")
            .ValidateOnStart();

        services.TryAddSingleton(resolver =>
            resolver.GetRequiredService<ILoggerFactory>().CreateLogger<AbonentClient>());

        services.AddGrpcClient<AbonentService.AbonentServiceClient>((sp, options) =>
        {
            var clientOptions = sp.GetRequiredService<IOptions<AbonentClientOptions>>().Value;
            options.Address = new Uri(clientOptions.Address);
        });

        services.TryAddSingleton<IAbonentClient, AbonentClient>();

        return services;
    }

    /// <summary>
    /// Registers the gRPC <see cref="AbonentService.AbonentServiceClient"/> and
    /// the <see cref="IAbonentClient"/> wrapper using an inline address.
    /// </summary>
    public static IServiceCollection AddAbonentClient(
        this IServiceCollection services,
        string address)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(address);

        services.AddOptions<AbonentClientOptions>()
            .Configure(o => o.Address = address);

        services.TryAddSingleton(resolver =>
            resolver.GetRequiredService<ILoggerFactory>().CreateLogger<AbonentClient>());

        services.AddGrpcClient<AbonentService.AbonentServiceClient>((sp, options) =>
        {
            var clientOptions = sp.GetRequiredService<IOptions<AbonentClientOptions>>().Value;
            options.Address = new Uri(clientOptions.Address);
        });

        services.TryAddSingleton<IAbonentClient, AbonentClient>();

        return services;
    }
}
