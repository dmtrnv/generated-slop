namespace AbonentClient;

/// <summary>
/// Configuration options for the <see cref="IAbonentClient"/>.
/// Bind from configuration section "AbonentClient" (or any section of your choice).
/// </summary>
public sealed class AbonentClientOptions
{
    /// <summary>
    /// Default configuration section name used by <see cref="ServiceCollectionExtensions.AddAbonentClient"/>.
    /// </summary>
    public const string SectionName = "AbonentClient";

    /// <summary>
    /// gRPC server address in the form "https://host:port".
    /// </summary>
    public string Address { get; set; } = "https://localhost:5001";
}
