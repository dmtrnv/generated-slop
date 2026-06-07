using System.Text.Json;
using Grpc.Core;
using OrderClient;
using OrderSource;
using QuerySource;

namespace AdapterFacade.Services;

public class OrderAdapter : IAdapter
{
    public static string SourceId { get; } = "order_adapter_source_id";

    private readonly IOrderClient _orderClient;
    private readonly ILogger<OrderAdapter> _logger;

    public OrderAdapter(IOrderClient orderClient, ILogger<OrderAdapter> logger)
    {
        _orderClient = orderClient ?? throw new ArgumentNullException(nameof(orderClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task Find(
        IEnumerable<string> phoneNumbers,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(phoneNumbers);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        var schema = Schema();

        foreach (var phoneNumber in phoneNumbers)
        {
            if (string.IsNullOrWhiteSpace(phoneNumber))
            {
                _logger.LogWarning("Skipping blank phone number in OrderAdapter.Find");
                continue;
            }

            if (context.CancellationToken.IsCancellationRequested)
            {
                break;
            }

            _logger.LogInformation("OrderAdapter searching for phone {Phone}", phoneNumber);

            try
            {
                await foreach (var order in _orderClient
                    .GetOrdersByPhoneAsync(phoneNumber, context.CancellationToken)
                    .WithCancellation(context.CancellationToken))
                {
                    if (context.CancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    var data = SerializeOrder(order);

                    _logger.LogInformation(
                        "Streaming order {OrderId} ({ProductName}) for phone {Phone}",
                        order.OrderId,
                        order.ProductName,
                        order.PhoneNumber);

                    await responseStream.WriteAsync(new QueryResponse
                    {
                        ResultSchema = schema,
                        Data = data,
                    });
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to fetch orders for phone {Phone} from OrderAdapter",
                    phoneNumber);
                throw new RpcException(
                    new Status(StatusCode.Internal, $"Failed to fetch orders: {ex.Message}"));
            }
        }
    }

    public string Schema()
    {
        // GraphQL SDL describing the Order entity produced by this adapter.
        // Mirrors the fields declared in Protos/order.proto (OrderInfo):
        //   - order_id     (string)
        //   - phone_number (string)
        //   - product_name (string)
        //   - amount       (double)
        return """
        type Order {
            order_id: String!
            phone_number: String!
            product_name: String!
            amount: Float!
        }

        type Query {
            searhByPhoneNumber(phone_number: String!): [Order!]!
        }
        """;
    }

    private static string SerializeOrder(OrderInfo order)
    {
        var payload = new
        {
            order_id = order.OrderId,
            phone_number = order.PhoneNumber,
            product_name = order.ProductName,
            amount = order.Amount,
        };

        return JsonSerializer.Serialize(payload);
    }
}
