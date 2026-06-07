namespace OrderClient;

/// <summary>
/// Configuration options for the <see cref="IOrderClient"/>.
/// Bind from configuration section "OrderClient" (or any section of your choice).
/// </summary>
public sealed class OrderClientOptions
{
    /// <summary>
    /// Default configuration section name used by <see cref="ServiceCollectionExtensions.AddOrderClient"/>.
    /// </summary>
    public const string SectionName = "OrderClient";

    /// <summary>
    /// gRPC server address in the form "https://host:port".
    /// </summary>
    public string Address { get; set; } = "https://localhost:5001";
}
