using GraphQL.Types;
using GraphQLParser.AST;
using Grpc.Core;
using OrderClient;
using OrderSource;
using QuerySource;

namespace AdapterFacade.Services;

public class OrderAdapter : IAdapter
{
    public static string SourceId { get; } = "order_adapter_source_id";

    // The entity's field types are defined exclusively in Schema();
    // this only identifies the type by name.
    private static readonly EntityTypeDefinition OrderType = new(
        graphqlTypeName: "Order");

    private readonly IOrderClient _orderClient;
    private readonly ILogger<OrderAdapter> _logger;

    public OrderAdapter(IOrderClient orderClient, ILogger<OrderAdapter> logger)
    {
        _orderClient = orderClient ?? throw new ArgumentNullException(nameof(orderClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task Find(
        IEnumerable<string> phoneNumbers,
        IReadOnlyList<GraphQLField> selectionSet,
        IReadOnlyList<AppliedDirective> directives,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(phoneNumbers);
        ArgumentNullException.ThrowIfNull(selectionSet);
        ArgumentNullException.ThrowIfNull(directives);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        // Directives from the top-level selection are forwarded as-is. We
        // do not interpret them here - adapters downstream may inspect
        // the list to alter behavior (caching, authorization, etc.).
        if (directives.Count > 0)
        {
            _logger.LogInformation(
                "OrderAdapter received {DirectiveCount} directive(s) on searhByPhoneNumber: {Directives}",
                directives.Count,
                string.Join(", ", directives.Select(d => d.Name)));
        }

        // The schema is derived from the selection set so that the
        // streamed response declares exactly the fields the client asked
        // for, in the order it asked for them. We rewrite the adapter's
        // existing SDL in place (preserving the Query type and any
        // surrounding whitespace) rather than synthesizing a new one.
        var schema = SelectionSetSerializer.RewriteSchema(
            Schema(),
            OrderType,
            selectionSet);

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

                    var data = SelectionSetSerializer.Serialize(
                        selectionSet,
                        order,
                        ResolveOrderField);

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
        // Full GraphQL SDL describing the Order entity produced by this
        // adapter. Mirrors the fields declared in Protos/order.proto
        // (OrderInfo):
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

    /// <summary>
    /// Returns the value of the named field on the given
    /// <see cref="OrderInfo"/>, or <c>null</c> when the field is not
    /// part of the Order entity (in which case the serializer omits it
    /// from the payload).
    /// </summary>
    private static object? ResolveOrderField(OrderInfo order, string fieldName) => fieldName switch
    {
        "order_id" => order.OrderId,
        "phone_number" => order.PhoneNumber,
        "product_name" => order.ProductName,
        "amount" => order.Amount,
        _ => null,
    };
}
