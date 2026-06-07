using OrderSource;

namespace OrderClient;

/// <summary>
/// Strongly-typed wrapper around the gRPC <see cref="OrderService.OrderServiceClient"/>
/// exposing domain-friendly methods to consumers without leaking generated client types.
/// </summary>
public interface IOrderClient
{
    /// <summary>
    /// Streams all orders whose phone number matches <paramref name="phoneNumber"/>.
    /// Honors the cancellation token for cooperative shutdown.
    /// </summary>
    /// <param name="phoneNumber">Phone number to look up.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async sequence of <see cref="OrderInfo"/> messages.</returns>
    IAsyncEnumerable<OrderInfo> GetOrdersByPhoneAsync(
        string phoneNumber,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Collects every streamed <see cref="OrderInfo"/> for the given phone number
    /// into an in-memory list. Useful for tests and small result sets.
    /// </summary>
    Task<IReadOnlyList<OrderInfo>> GetOrdersByPhoneListAsync(
        string phoneNumber,
        CancellationToken cancellationToken = default);
}
