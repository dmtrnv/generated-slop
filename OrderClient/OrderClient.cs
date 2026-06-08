using System.Runtime.CompilerServices;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using OrderSource;

namespace OrderClient;

/// <summary>
/// Default implementation of <see cref="IOrderClient"/> that delegates to
/// the typed gRPC <see cref="OrderService.OrderServiceClient"/> produced
/// by <c>AddGrpcClient<T></c>.
/// </summary>
public sealed class OrderClient : IOrderClient
{
    private readonly OrderService.OrderServiceClient _client;
    private readonly ILogger<OrderClient> _logger;

    public OrderClient(
        OrderService.OrderServiceClient client,
        ILogger<OrderClient> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async IAsyncEnumerable<OrderInfo> GetOrdersByPhoneAsync(
        string phoneNumber,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(phoneNumber))
        {
            throw new ArgumentException("Phone number must be provided.", nameof(phoneNumber));
        }

        _logger.LogInformation("Requesting orders for phone {Phone}", phoneNumber);

        var request = new PhoneRequest { PhoneNumber = phoneNumber };
        var call = _client.GetOrdersByPhone(request, cancellationToken: cancellationToken);

        await foreach (var info in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return info;
        }
    }

    public async Task<IReadOnlyList<OrderInfo>> GetOrdersByPhoneListAsync(
        string phoneNumber,
        CancellationToken cancellationToken = default)
    {
        var result = new List<OrderInfo>();
        await foreach (var info in GetOrdersByPhoneAsync(phoneNumber, cancellationToken))
        {
            result.Add(info);
        }
        return result;
    }

    public async IAsyncEnumerable<OrderInfo> GetOrdersByOrderIdAsync(
        string orderId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(orderId))
        {
            throw new ArgumentException("Order id must be provided.", nameof(orderId));
        }

        _logger.LogInformation("Requesting orders for order_id {OrderId}", orderId);

        var request = new OrderIdRequest { OrderId = orderId };
        var call = _client.GetOrdersByOrderId(request, cancellationToken: cancellationToken);

        await foreach (var info in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return info;
        }
    }
}
