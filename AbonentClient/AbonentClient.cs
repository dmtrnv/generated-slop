using System.Runtime.CompilerServices;
using AbonentSource;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AbonentClient;

/// <summary>
/// Default implementation of <see cref="IAbonentClient"/> that delegates to
/// the typed gRPC <see cref="AbonentService.AbonentServiceClient"/> produced
/// by <c>AddGrpcClient<T></c>.
/// </summary>
public sealed class AbonentClient : IAbonentClient
{
    private readonly AbonentService.AbonentServiceClient _client;
    private readonly ILogger<AbonentClient> _logger;

    public AbonentClient(
        AbonentService.AbonentServiceClient client,
        ILogger<AbonentClient> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async IAsyncEnumerable<AbonentInfo> GetAbonentsByPhoneAsync(
        string phoneNumber,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(phoneNumber))
        {
            throw new ArgumentException("Phone number must be provided.", nameof(phoneNumber));
        }

        _logger.LogInformation("Requesting abonents for phone {Phone}", phoneNumber);

        var request = new PhoneRequest { PhoneNumber = phoneNumber };
        var call = _client.GetAbonentsByPhone(request, cancellationToken: cancellationToken);

        await foreach (var info in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return info;
        }
    }

    public async Task<IReadOnlyList<AbonentInfo>> GetAbonentsByPhoneListAsync(
        string phoneNumber,
        CancellationToken cancellationToken = default)
    {
        var result = new List<AbonentInfo>();
        await foreach (var info in GetAbonentsByPhoneAsync(phoneNumber, cancellationToken))
        {
            result.Add(info);
        }
        return result;
    }
}
