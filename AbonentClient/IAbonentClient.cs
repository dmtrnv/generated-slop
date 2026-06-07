using AbonentSource;

namespace AbonentClient;

/// <summary>
/// Strongly-typed wrapper around the gRPC <see cref="AbonentService.AbonentServiceClient"/>
/// exposing domain-friendly methods to consumers without leaking generated client types.
/// </summary>
public interface IAbonentClient
{
    /// <summary>
    /// Streams all abonents whose phone number matches <paramref name="phoneNumber"/>.
    /// Honors the cancellation token for cooperative shutdown.
    /// </summary>
    /// <param name="phoneNumber">Phone number to look up.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async sequence of <see cref="AbonentInfo"/> messages.</returns>
    IAsyncEnumerable<AbonentInfo> GetAbonentsByPhoneAsync(
        string phoneNumber,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Collects every streamed <see cref="AbonentInfo"/> for the given phone number
    /// into an in-memory list. Useful for tests and small result sets.
    /// </summary>
    Task<IReadOnlyList<AbonentInfo>> GetAbonentsByPhoneListAsync(
        string phoneNumber,
        CancellationToken cancellationToken = default);
}
