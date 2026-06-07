using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OrderSource;

namespace OrderClient;

/// <summary>
/// DI helpers for registering <see cref="IOrderClient"/> in a host application.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the gRPC <see cref="OrderService.OrderServiceClient"/> and
    /// the <see cref="IOrderClient"/> wrapper using the default
    /// <see cref="OrderClientOptions.SectionName"/> configuration section.
    /// </summary>
    public static IServiceCollection AddOrderClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        return services.AddOrderClient(configuration, OrderClientOptions.SectionName);
    }

    /// <summary>
    /// Registers the gRPC <see cref="OrderService.OrderServiceClient"/> and
    /// the <see cref="IOrderClient"/> wrapper using a custom configuration section.
    /// </summary>
    public static IServiceCollection AddOrderClient(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);

        services
            .AddOptions<OrderClientOptions>()
            .Bind(configuration.GetSection(sectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.Address),
                $"{nameof(OrderClientOptions.Address)} must be configured.")
            .ValidateOnStart();

        services.TryAddSingleton(resolver =>
            resolver.GetRequiredService<ILoggerFactory>().CreateLogger<OrderClient>());

        services.AddGrpcClient<OrderService.OrderServiceClient>((sp, options) =>
        {
            var clientOptions = sp.GetRequiredService<IOptions<OrderClientOptions>>().Value;
            options.Address = new Uri(clientOptions.Address);
        });

        services.TryAddSingleton<IOrderClient, OrderClient>();

        return services;
    }

    /// <summary>
    /// Registers the gRPC <see cref="OrderService.OrderServiceClient"/> and
    /// the <see cref="IOrderClient"/> wrapper using an inline address.
    /// </summary>
    public static IServiceCollection AddOrderClient(
        this IServiceCollection services,
        string address)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(address);

        services.AddOptions<OrderClientOptions>()
            .Configure(o => o.Address = address);

        services.TryAddSingleton(resolver =>
            resolver.GetRequiredService<ILoggerFactory>().CreateLogger<OrderClient>());

        services.AddGrpcClient<OrderService.OrderServiceClient>((sp, options) =>
        {
            var clientOptions = sp.GetRequiredService<IOptions<OrderClientOptions>>().Value;
            options.Address = new Uri(clientOptions.Address);
        });

        services.TryAddSingleton<IOrderClient, OrderClient>();

        return services;
    }
}
