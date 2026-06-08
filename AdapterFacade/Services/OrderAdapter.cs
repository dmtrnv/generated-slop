using System.Text.Json;
using GraphQL;
using GraphQL.Execution;
using GraphQL.Types;
using GraphQL.Utilities;
using GraphQLParser;
using GraphQLParser.Visitors;
using Grpc.Core;
using OrderClient;
using OrderSource;
using QuerySource;

namespace AdapterFacade.Services;

/// <summary>
/// Adapter that exposes the <c>OrderSource</c> data over a code-first
/// GraphQL schema. Supports two lookup root fields:
/// <list type="bullet">
///   <item><c>searhByPhoneNumber(phone_number: String!): [Order!]!</c></item>
///   <item><c>findByOrderId(order_id: String!): [Order!]!</c></item>
/// </list>
/// Queries are executed in-process via <see cref="IDocumentExecuter"/>; the
/// resulting selection-set-projected rows are streamed back one-by-one as JSON
/// <see cref="QueryResponse"/> messages with the printed schema attached.
/// </summary>
public class OrderAdapter : IAdapter
{
    public static string SourceId { get; } = "order_adapter_source_id";

    private readonly IOrderClient _orderClient;
    private readonly ILogger<OrderAdapter> _logger;
    private readonly IDocumentExecuter _executor;

    // Lazily-initialized, immutable, thread-safe after construction. Building
    // the schema is relatively expensive so we do it once per adapter.
    private readonly Lazy<Schema> _schema;

    public OrderAdapter(IOrderClient orderClient, ILogger<OrderAdapter> logger)
        : this(orderClient, logger, new DocumentExecuter())
    {
    }

    // Internal constructor for testability / explicit executors.
    internal OrderAdapter(
        IOrderClient orderClient,
        ILogger<OrderAdapter> logger,
        IDocumentExecuter executor)
    {
        _orderClient = orderClient ?? throw new ArgumentNullException(nameof(orderClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _schema = new Lazy<Schema>(BuildSchema, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public async Task Find(
        AdapterQuery query,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        if (string.IsNullOrWhiteSpace(query.Query))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "query must be provided"));
        }

        var schema = _schema.Value;
        var sdl = schema.Print();

        ExecutionResult executionResult;
        try
        {
            executionResult = await _executor.ExecuteAsync(opts =>
            {
                opts.Schema = schema;
                opts.Query = query.Query;
                opts.Variables = query.Variables;
                opts.OperationName = query.OperationName;
                opts.CancellationToken = context.CancellationToken;
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GraphQL execution failed in OrderAdapter");
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }

        if (executionResult.Errors is { Count: > 0 })
        {
            var message = string.Join("; ", executionResult.Errors.Select(e => e.Message));
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, $"Query execution failed: {message}"));
        }

        // GraphQL.NET 8 exposes the result of IDocumentExecuter as an
        // ExecutionNode graph (RootExecutionNode at the top), NOT a
        // Dictionary<string, object?>. We walk the graph to extract the
        // selection-set-projected rows for the single root field the
        // client asked for.
        if (executionResult.Data is not RootExecutionNode rootNode || rootNode.SubFields is null || rootNode.SubFields.Length == 0)
        {
            return;
        }
        
        // Pick the first root-level field — for a single-operation query
        // (which is what the QueryService validates for) there is exactly
        // one. We log which field the executor actually dispatched to.
        var rootFieldNode = rootNode.SubFields[0];
        var rootField = rootFieldNode.Name;

        _logger.LogInformation("OrderAdapter dispatched to root field {RootField}", rootField);

        // Collect the projected rows. The selected root field is a list
        // returning field ([Order!]!), so the executor produced an
        // ArrayExecutionNode; for safety we also support a degenerate
        // single-row case.
        var rows = ExtractRows(rootFieldNode);

        try
        {
            foreach (var row in rows)
            {
                if (context.CancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (row is null)
                {
                    continue;
                }

                var data = JsonSerializer.Serialize(row);

                _logger.LogInformation(
                    "Streaming projected order row from root field {RootField}",
                    rootField);

                await responseStream.WriteAsync(new QueryResponse
                {
                    ResultSchema = sdl,
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
            _logger.LogError(ex, "Failed to stream orders from OrderAdapter");
            throw new RpcException(
                new Status(StatusCode.Internal, $"Failed to stream orders: {ex.Message}"));
        }
    }

    /// <summary>
    /// Pulls the selection-set-projected row objects out of the
    /// <see cref="ExecutionNode"/> that the executor produced for the
    /// root field. For an <see cref="ArrayExecutionNode"/> each item
    /// becomes one row; otherwise the node itself is treated as a
    /// single row.
    /// </summary>
    private static IEnumerable<object?> ExtractRows(ExecutionNode rootFieldNode)
    {
        if (rootFieldNode is ArrayExecutionNode arrayNode && arrayNode.Items is { Count: > 0 } items)
        {
            foreach (var item in items)
            {
                yield return ProjectNode(item);
            }
            yield break;
        }

        yield return ProjectNode(rootFieldNode);
    }

    /// <summary>
    /// Converts an <see cref="ExecutionNode"/> into the dictionary
    /// shape we want to JSON-serialize. For an <see cref="ObjectExecutionNode"/>
    /// we use its <c>SubFields</c> — each child's <c>Name</c> and
    /// projected <c>Result</c> become one entry, applying the
    /// selection set automatically (only selected fields appear in
    /// SubFields). Scalar child results are returned as-is.
    /// </summary>
    private static object? ProjectNode(ExecutionNode? node)
    {
        if (node is null) return null;

        if (node is ObjectExecutionNode objNode && objNode.SubFields is { Length: > 0 } subs)
        {
            var dict = new Dictionary<string, object?>(subs.Length);
            foreach (var sub in subs)
            {
                if (sub.Name is not null)
                {
                    dict[sub.Name] = sub.Result;
                }
            }
            return dict;
        }

        return node.Result;
    }

    public string Schema()
    {
        // Printable SDL produced from the code-first schema. Preserved as the
        // gRPC contract's ResultSchema value so existing consumers still see
        // a valid schema string.
        return _schema.Value.Print();
    }

    /// <summary>
    /// Composes the code-first GraphQL schema (object type + query root).
    /// </summary>
    private Schema BuildSchema()
    {
        var orderType = new OrderType();
        var query = new OrderQuery(_orderClient, _logger);
        return new OrderAdapterSchema(query, orderType);
    }

    // ---------------------------------------------------------------------
    // Code-first GraphQL types
    // ---------------------------------------------------------------------

    /// <summary>
    /// Strongly-typed DTO returned by resolvers. Fields are mapped 1-to-1 onto
    /// the proto's <see cref="OrderInfo"/>; the GraphQL field names are
    /// declared in <see cref="OrderType"/>.
    /// </summary>
    public sealed record OrderDto(
        string OrderId,
        string PhoneNumber,
        string ProductName,
        double Amount);

    private sealed class OrderType : ObjectGraphType<OrderDto>
    {
        public OrderType()
        {
            Name = "Order";
            Description = "Order entity produced by the order source.";

            // Field names mirror the proto's OrderInfo fields; using the
            // named overload is the v8+ recommended syntax (the chained
            // .Name(...) form is obsolete and emits GQL001 warnings).
            Field<StringGraphType>("order_id").Resolve(ctx => ctx.Source.OrderId);
            Field<StringGraphType>("phone_number").Resolve(ctx => ctx.Source.PhoneNumber);
            Field<StringGraphType>("product_name").Resolve(ctx => ctx.Source.ProductName);
            Field<FloatGraphType>("amount").Resolve(ctx => ctx.Source.Amount);
        }
    }

    private sealed class OrderQuery : ObjectGraphType
    {
        public OrderQuery(IOrderClient orderClient, ILogger logger)
        {
            Name = "Query";
            Description = "Root query for the order source.";

            Field<ListGraphType<OrderType>>("searhByPhoneNumber")
                .Description("Streams orders matching the supplied phone number.")
                .Argument<NonNullGraphType<StringGraphType>>("phone_number",
                    "Phone number used to look up orders.")
                .ResolveAsync(async context =>
                {
                    var phone = context.GetArgument<string>("phone_number");
                    if (string.IsNullOrWhiteSpace(phone))
                    {
                        context.Errors.Add(new ExecutionError("phone_number must be provided."));
                        return Array.Empty<OrderDto>();
                    }

                    logger.LogInformation(
                        "Resolving searhByPhoneNumber(phone_number: {Phone})",
                        phone);

                    var results = new List<OrderDto>();
                    await foreach (var info in orderClient
                        .GetOrdersByPhoneAsync(phone, context.CancellationToken)
                        .WithCancellation(context.CancellationToken))
                    {
                        results.Add(ToDto(info));
                    }
                    return results;
                });

            Field<ListGraphType<OrderType>>("findByOrderId")
                .Description("Streams orders matching the supplied order id.")
                .Argument<NonNullGraphType<StringGraphType>>("order_id",
                    "Order identifier used to look up orders.")
                .ResolveAsync(async context =>
                {
                    var orderId = context.GetArgument<string>("order_id");
                    if (string.IsNullOrWhiteSpace(orderId))
                    {
                        context.Errors.Add(new ExecutionError("order_id must be provided."));
                        return Array.Empty<OrderDto>();
                    }

                    logger.LogInformation(
                        "Resolving findByOrderId(order_id: {OrderId})",
                        orderId);

                    var results = new List<OrderDto>();
                    await foreach (var info in orderClient
                        .GetOrdersByOrderIdAsync(orderId, context.CancellationToken)
                        .WithCancellation(context.CancellationToken))
                    {
                        results.Add(ToDto(info));
                    }
                    return results;
                });
        }
    }

    private sealed class OrderAdapterSchema : Schema
    {
        public OrderAdapterSchema(OrderQuery query, OrderType orderType)
        {
            Query = query;
            RegisterType(orderType);
        }
    }

    private static OrderDto ToDto(OrderInfo info) =>
        new(info.OrderId, info.PhoneNumber, info.ProductName, info.Amount);
}
