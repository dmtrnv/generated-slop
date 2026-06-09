using System.Runtime.CompilerServices;
using System.Text.Json;
using GraphQL;
using GraphQL.Execution;
using GraphQL.Resolvers;
using GraphQL.Types;
using GraphQL.Utilities;
using Grpc.Core;
using OrderClient;
using OrderSource;
using QuerySource;

namespace AdapterFacade.Services;

/// <summary>
/// Adapter that exposes the <c>OrderSource</c> data over a code-first
/// GraphQL schema. Supports two lookup root fields on both the
/// <c>Query</c> and <c>Subscription</c> root types:
/// <list type="bullet">
///   <item><c>searhByPhoneNumber(phone_number: String!): [Order!]!</c> / <c>: Order!</c></item>
///   <item><c>findByOrderId(order_id: String!): [Order!]!</c> / <c>: Order!</c></item>
/// </list>
/// <para>
/// The query fields materialize the upstream <see cref="IAsyncEnumerable{OrderInfo}"/>
/// into a <see cref="List{OrderDto}"/> before returning (GraphQL.NET 8's
/// <c>ExecutionStrategy.SetArrayItemNodesAsync</c> requires a synchronous
/// <see cref="System.Collections.IEnumerable"/> for list-field resolvers).
/// The subscription fields, in contrast, return
/// <see cref="IAsyncEnumerable{OrderDto}"/> directly and are driven by
/// <see cref="SubscriptionExecutionStrategy"/>, which iterates per source
/// event and projects each item through the selection set.
/// </para>
/// <para>
/// Per the project's Plan C (see <c>plans/order-adapter-resolver-streaming-options.md</c>)
/// the gRPC carrier (<see cref="IServerStreamWriter{QueryResponse}"/>) is
/// unchanged: <see cref="Find"/> still writes one <see cref="QueryResponse"/>
/// per projected row. End-to-end resolver-level streaming only applies
/// to the in-process subscription execution path; the gRPC subscription
/// path still goes through the executor's <c>IDocumentExecuter</c>, which
/// per the design notes in Plan C §C.6 risk 1 may buffer the final
/// <see cref="ExecutionResult"/>.
/// </para>
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

    // Public constructor used by DI. The executor is injected so the
    // subscription execution strategy can be wired up in Program.cs.
    public OrderAdapter(
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

        // Subscription path: GraphQL.NET 8 returns Data == null and
        // populates Streams with one IObservable<ExecutionResult> per
        // subscription root field. We forward each emitted
        // ExecutionResult to the gRPC carrier as its own QueryResponse.
        // Each emitted ExecutionResult.Data is a RootExecutionNode graph
        // (same shape as query operations) — the SelectionSet has
        // already been applied to the graph, but the graph carries
        // back-references to the GraphQLDocument that hold
        // ROM<char> ref-struct properties System.Text.Json can't
        // serialize, so the observer projects the graph into a clean
        // Dictionary<string, object?> before JSON-serializing.
        if (executionResult.Streams is { Count: > 0 } streams)
        {
            await StreamSubscriptionAsync(streams, sdl, responseStream, context);
            return;
        }

        // Query path: GraphQL.NET 8 exposes the result of IDocumentExecuter
        // as an ExecutionNode graph (RootExecutionNode at the top), NOT a
        // Dictionary<string, object?>. We walk the graph to extract the
        // selection-set-projected rows for the single root field the
        // client asked for.
        if (executionResult.Data is not RootExecutionNode rootNode || rootNode.SubFields is null || rootNode.SubFields.Length == 0)
        {
            return;
        }

        // Pick the first root-level field — for a single-operation query
        // or subscription (which is what the QueryService validates for)
        // there is exactly one. We log which field the executor actually
        // dispatched to.
        var rootFieldNode = rootNode.SubFields[0];
        var rootField = rootFieldNode.Name;

        _logger.LogInformation("OrderAdapter dispatched to root field {RootField}", rootField);

        // For query operations the root field is a list returning
        // field ([Order!]!), so the executor produced an
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
    /// Forwards each per-event <see cref="ExecutionResult"/> emitted by a
    /// subscription operation's <see cref="IObservable{T}"/> stream out
    /// onto the gRPC <see cref="IServerStreamWriter{QueryResponse}"/>
    /// carrier as a single <see cref="QueryResponse"/> message. Honors
    /// the call's <see cref="ServerCallContext.CancellationToken"/>: on
    /// cancellation we dispose the subscription, which signals the
    /// upstream resolver to stop producing events.
    /// </summary>
    /// <remarks>
    /// The <see cref="SubscriptionObserver"/> projects each emitted
    /// <see cref="ExecutionResult"/> (whose <c>Data</c> is a
    /// <see cref="RootExecutionNode"/> graph with the selection set
    /// already applied) into a JSON-ready
    /// <c>Dictionary&lt;string, object?&gt;</c> before forwarding.
    /// Errors observed on the stream are converted into an
    /// <see cref="RpcException"/> with <see cref="StatusCode.Internal"/>.
    /// </remarks>
    private async Task StreamSubscriptionAsync(
        IDictionary<string, IObservable<ExecutionResult>> streams,
        string sdl,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(streams);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        // The GraphQL spec says a subscription document has exactly one
        // root field, and GraphQL.NET 8 enforces this by producing a
        // Streams dictionary with exactly one entry. We still handle
        // the multi-entry case defensively (iterate all entries) and
        // log which root field each stream is for.
        var completion = new TaskCompletionSource<Exception?>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // Track every subscription so we can dispose them all on
        // cancellation. We collect them into a local array rather than
        // capturing into the closure so that the IObserver side can
        // also write to it safely.
        var subscriptions = new IDisposable[streams.Count];
        var pendingCount = streams.Count;

        var rootFields = streams.Keys.ToArray();
        for (var i = 0; i < rootFields.Length; i++)
        {
            var rootField = rootFields[i];
            var observable = streams[rootField];

            _logger.LogInformation(
                "OrderAdapter subscribing to root field {RootField}",
                rootField);

            var observer = new SubscriptionObserver(
                rootField,
                sdl,
                responseStream,
                _logger,
                onTerminal: () =>
                {
                    // Each stream completes (or errors) independently;
                    // signal overall completion when the last one ends.
                    if (Interlocked.Decrement(ref pendingCount) == 0)
                    {
                        completion.TrySetResult(null);
                    }
                },
                onTerminalWithError: ex =>
                {
                    // First error wins; subsequent completions are no-ops.
                    completion.TrySetResult(ex);
                });

            subscriptions[i] = observable.Subscribe(observer);
        }

        // Register cancellation: dispose every subscription, which
        // signals the upstream producer to stop.
        await using var cancelRegistration = context.CancellationToken
            .Register(() =>
            {
                foreach (var s in subscriptions)
                {
                    try { s.Dispose(); } catch { /* swallow */ }
                }
                completion.TrySetResult(null);
            })
            .ConfigureAwait(false);

        var terminalError = await completion.Task.ConfigureAwait(false);
        if (terminalError is not null)
        {
            _logger.LogError(terminalError, "Subscription stream failed in OrderAdapter");
            throw new RpcException(
                new Status(StatusCode.Internal, $"Subscription stream failed: {terminalError.Message}"));
        }
    }

    /// <summary>
    /// Bridges an <see cref="IObservable{ExecutionResult}"/> to the gRPC
    /// response stream. Each <c>OnNext</c> projects the
    /// <see cref="ExecutionResult.Data"/> <see cref="RootExecutionNode"/>
    /// graph (already selection-set-projected by
    /// <see cref="SubscriptionExecutionStrategy"/>) into a
    /// JSON-ready <c>Dictionary&lt;string, object?&gt;</c> using
    /// <see cref="ProjectRootNode"/>, serializes it, and writes the
    /// resulting JSON inside a <see cref="QueryResponse"/>. Synchronous
    /// writes are offloaded so the observable's producer thread is
    /// never blocked by gRPC backpressure.
    /// </summary>
    private sealed class SubscriptionObserver : IObserver<ExecutionResult>
    {
        private readonly string _rootField;
        private readonly string _sdl;
        private readonly IServerStreamWriter<QueryResponse> _responseStream;
        private readonly ILogger _logger;
        private readonly Action _onTerminal;
        private readonly Action<Exception> _onTerminalWithError;

        public SubscriptionObserver(
            string rootField,
            string sdl,
            IServerStreamWriter<QueryResponse> responseStream,
            ILogger logger,
            Action onTerminal,
            Action<Exception> onTerminalWithError)
        {
            _rootField = rootField;
            _sdl = sdl;
            _responseStream = responseStream;
            _logger = logger;
            _onTerminal = onTerminal;
            _onTerminalWithError = onTerminalWithError;
        }

        public void OnNext(ExecutionResult value)
        {
            try
            {
                // Defensively skip events with no data (matches the query
                // path's null-row handling).
                if (value is null || value.Data is null)
                {
                    _logger.LogWarning(
                        "Subscription event for root field {RootField} had null Data; skipping",
                        _rootField);
                    return;
                }

                // The SubscriptionExecutionStrategy produces ExecutionResult.Data
                // as a RootExecutionNode graph (same shape as query operations),
                // NOT a pre-projected Dictionary<string, object?>. The graph
                // nodes carry a back-reference to the GraphQLDocument, which
                // contains ROM<char> ref-struct properties that
                // System.Text.Json refuses to serialize (see
                // System.Text.Json.ThrowHelper.ThrowInvalidOperationException_CannotSerializeInvalidType).
                // Walk the graph to extract a clean, JSON-ready dictionary
                // that respects the selection set — same approach as the
                // query path's ExtractRows / ProjectNode helpers.
                //
                // IMPORTANT: per-event we want each QueryResponse.Data to
                // contain the single Order's selection-set projection
                // (e.g. {"order_id":"...","phone_number":"..."}), NOT the
                // subscription root field wrapper (e.g.
                // {"searhByPhoneNumber":{...}}). The gRPC carrier contract
                // is one row per QueryResponse, and the client already
                // knows which subscription root field it asked for (it's
                // in the operation text). The query path follows the same
                // rule via ExtractRows: it skips the root field node and
                // yields its projected items, never the root wrapper.
                object? projected = value.Data is RootExecutionNode rootNode
                    ? ProjectFirstRootChild(rootNode)
                    : value.Data;

                var data = JsonSerializer.Serialize(projected);

                _logger.LogInformation(
                    "Streaming projected order row from subscription root field {RootField}",
                    _rootField);

                // The observable may invoke OnNext on a non-async context
                // (e.g. a thread-pool worker used by Rx). gRPC's
                // WriteAsync is awaitable but we mustn't block the
                // observable's producer — start the write and continue.
                // The awaiter is dropped deliberately: completion is
                // driven by OnCompleted / OnError, and any individual
                // write error is propagated via the shared completion.
                _ = WriteAsync(data);
            }
            catch (Exception ex)
            {
                // Defensive: any synchronous failure (e.g. serialization
                // error) terminates the stream with an error.
                _onTerminalWithError(ex);
            }
        }

        private async Task WriteAsync(string data)
        {
            try
            {
                await _responseStream.WriteAsync(new QueryResponse
                {
                    ResultSchema = _sdl,
                    Data = data,
                }).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _onTerminalWithError(ex);
            }
        }

        public void OnError(Exception error) => _onTerminalWithError(error);

        public void OnCompleted() => _onTerminal();

        /// <summary>
        /// Converts the <see cref="RootExecutionNode"/> that
        /// <see cref="SubscriptionExecutionStrategy"/> emits on each
        /// subscription event into a JSON-ready value matching the
        /// project's gRPC carrier contract: one <see cref="QueryResponse"/>
        /// per projected Order row, with the payload containing the
        /// selection-set-projected sub-fields directly (e.g.
        /// <c>{"order_id":"...","phone_number":"..."}</c>) — not wrapped
        /// in the subscription root field name.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The subscription document has exactly one root field (e.g.
        /// <c>searhByPhoneNumber</c>). The executor's per-event graph
        /// for the root has the Order's leaf execution nodes attached
        /// either directly to the root field's
        /// <see cref="ObjectExecutionNode.SubFields"/> (most common:
        /// the field type is <c>Order!</c> so the executor creates an
        /// <see cref="ObjectExecutionNode"/> at the root field level
        /// with the selection-set leaves as its <c>SubFields</c>) or
        /// nested one level deeper (defensive fallback). We rely on
        /// the executor's selection-set application: only the
        /// requested leaf fields appear in the relevant
        /// <see cref="ObjectExecutionNode.SubFields"/> array.
        /// </para>
        /// <para>
        /// The graph nodes hold a back-reference to the
        /// <c>GraphQLDocument</c> which contains <c>ROM&lt;char&gt;</c>
        /// ref-struct properties that <see cref="JsonSerializer"/>
        /// cannot serialize, so we must project the graph into plain
        /// objects before handing it to <see cref="JsonSerializer"/>.
        /// </para>
        /// </remarks>
        private static object? ProjectFirstRootChild(RootExecutionNode rootNode)
        {
            if (rootNode.SubFields is null || rootNode.SubFields.Length == 0)
            {
                return null;
            }

            var rootFieldNode = rootNode.SubFields[0];

            // Fast path: the subscription root field's value is an
            // Order (object type) and the executor materializes it as
            // an ObjectExecutionNode whose SubFields are the
            // selection-set-projected leaf fields. Project them
            // directly.
            if (rootFieldNode is ObjectExecutionNode rootObj
                && rootObj.SubFields is { Length: > 0 } rootObjSubs)
            {
                return BuildSelectionDict(rootObjSubs);
            }

            // Defensive fallback: if the root field is wrapped in a
            // ValueExecutionNode (e.g. if the executor flattens
            // single-object subscriptions differently), drill one
            // level deeper looking for the Order's ObjectExecutionNode.
            // (ExecutionNode has no SubFields property, so this
            // branch is only reachable if the runtime node type
            // happens to expose a SubFields array via duck-typing
            // on one of the derived classes; the cast handles the
            // common case.)
            if (rootFieldNode is ObjectExecutionNode wrappedObj
                && wrappedObj.SubFields is { Length: > 0 } wrappedSubs)
            {
                foreach (var inner in wrappedSubs)
                {
                    if (inner is ObjectExecutionNode orderNode
                        && orderNode.SubFields is { Length: > 0 } orderSubs)
                    {
                        return BuildSelectionDict(orderSubs);
                    }
                }
            }

            // Last resort: return the root field's Result as-is
            // (typically the OrderDto for a ValueExecutionNode path).
            // Serialization of a record DTO yields a JSON object
            // with the Order's fields, which still respects the
            // selection set because the Order's child resolvers have
            // already populated the per-event Data.
            return rootFieldNode.Result;
        }

        /// <summary>
        /// Builds a JSON-ready dictionary from a list of leaf
        /// <see cref="ExecutionNode"/> instances, mapping each
        /// non-null <c>Name</c> to its selection-set-projected
        /// <c>Result</c>. Used by <see cref="ProjectFirstRootChild"/>
        /// to extract the Order's selection-set projection from the
        /// subscription's per-event graph.
        /// </summary>
        private static Dictionary<string, object?> BuildSelectionDict(
            ExecutionNode[] subs)
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
        // a valid schema string. The printed schema now also includes the
        // `type Subscription { ... }` block (Plan C, §C.3.1).
        return _schema.Value.Print();
    }

    /// <summary>
    /// The adapter's live code-first schema instance. QueryService uses this
    /// directly for validation so that subscription <c>StreamResolver</c>s
    /// (which cannot be round-tripped through SDL) are preserved.
    /// </summary>
    public ISchema GraphQLSchema => _schema.Value;

    /// <summary>
    /// Composes the code-first GraphQL schema (object type, query root, and
    /// subscription root). The query root mirrors the original Plan A
    /// behaviour (in-memory <see cref="List{OrderDto}"/>); the
    /// subscription root exposes the same fields with
    /// <see cref="IAsyncEnumerable{OrderDto}"/> resolvers (Plan C).
    /// </summary>
    private Schema BuildSchema()
    {
        var orderType = new OrderType();
        var query = new OrderQuery(_orderClient, _logger);
        var subscription = new OrderSubscription(_orderClient, _logger);
        return new OrderAdapterSchema(query, orderType, subscription);
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

                    // GraphQL.NET 8's ExecutionStrategy.SetArrayItemNodesAsync
                    // requires the resolver's Result to be a synchronous
                    // IEnumerable (see ExecutionStrategy.cs:425). The streaming
                    // gains for this adapter therefore come from the outer
                    // pipeline (IServerStreamWriter<QueryResponse>), not from
                    // the resolver itself. See plans/order-adapter-streaming-resolvers.md.
                    // Plan C adds a separate subscription root that streams
                    // IAsyncEnumerable<OrderDto> directly.
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

                    // See note in searhByPhoneNumber: GraphQL.NET 8 requires
                    // IEnumerable at the resolver level, so the streaming wins
                    // are on the outer IServerStreamWriter<QueryResponse> path.
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

    /// <summary>
    /// Subscription root with two fields whose resolvers yield
    /// <see cref="IAsyncEnumerable{OrderDto}"/> directly from the upstream
    /// gRPC stream. Under <see cref="SubscriptionExecutionStrategy"/> the
    /// executor iterates the stream per source event, projects each item
    /// through the selection set, and produces one <see cref="QueryResponse"/>
    /// per projected row in the outer gRPC carrier.
    /// </summary>
    private sealed class OrderSubscription : ObjectGraphType
    {
        public OrderSubscription(IOrderClient orderClient, ILogger logger)
        {
            Name = "Subscription";
            Description = "Streaming subscriptions for the order source.";

            // searhByPhoneNumber(phone_number: String!): Order!
            AddField(new FieldType
            {
                Name = "searhByPhoneNumber",
                Description = "Streams one Order event per match. Replaces the in-memory list resolver.",
                Type = typeof(OrderType),
                Arguments = new QueryArguments(
                    new QueryArgument<NonNullGraphType<StringGraphType>>
                    {
                        Name = "phone_number",
                        Description = "Phone number used to look up orders.",
                    }),
                // In GraphQL.NET 8 a subscription field needs BOTH a
                // Resolver and a StreamResolver defined. The
                // StreamResolver is the IObservable<OrderDto> source
                // of events; per emitted event the Resolver runs with
                // ctx.Source set to the emitted value and its
                // returned value becomes the subscription root
                // field's Result. The ObjectExecutionNode then uses
                // that Result as the source for the child field
                // resolvers (order_id, phone_number, ...). So the
                // Resolver must pass through ctx.Source — returning
                // default(OrderDto) would yield a null field Result
                // and an empty selection set, which is exactly the
                // "null results, missing sub-fields" failure mode
                // this method avoids.
                Resolver = new FuncFieldResolver<OrderDto?>(ctx =>
                    new ValueTask<OrderDto?>(ctx.Source as OrderDto)),
                StreamResolver = new SourceStreamResolver<OrderDto>(ctx =>
                    AsyncEnumerableObservable.ToObservable(
                        MapToDtosAsync(
                            orderClient.GetOrdersByPhoneAsync(
                                ctx.GetArgument<string>("phone_number")!,
                                ctx.CancellationToken),
                            ctx.CancellationToken),
                        ctx.CancellationToken)),
            });

            // findByOrderId(order_id: String!): Order!
            AddField(new FieldType
            {
                Name = "findByOrderId",
                Description = "Streams one Order event per match. Replaces the in-memory list resolver.",
                Type = typeof(OrderType),
                Arguments = new QueryArguments(
                    new QueryArgument<NonNullGraphType<StringGraphType>>
                    {
                        Name = "order_id",
                        Description = "Order identifier used to look up orders.",
                    }),
                Resolver = new FuncFieldResolver<OrderDto?>(ctx =>
                    new ValueTask<OrderDto?>(ctx.Source as OrderDto)),
                StreamResolver = new SourceStreamResolver<OrderDto>(ctx =>
                    AsyncEnumerableObservable.ToObservable(
                        MapToDtosAsync(
                            orderClient.GetOrdersByOrderIdAsync(
                                ctx.GetArgument<string>("order_id")!,
                                ctx.CancellationToken),
                            ctx.CancellationToken),
                        ctx.CancellationToken)),
            });
        }
    }

    private sealed class OrderAdapterSchema : Schema
    {
        public OrderAdapterSchema(
            OrderQuery query,
            OrderType orderType,
            OrderSubscription subscription)
        {
            Query = query;
            Subscription = subscription;
            RegisterType(orderType);
        }
    }

    private static OrderDto ToDto(OrderInfo info) =>
        new(info.OrderId, info.PhoneNumber, info.ProductName, info.Amount);

    /// <summary>
    /// Yields one <see cref="OrderDto"/> per upstream
    /// <see cref="OrderInfo"/>, honoring the caller's cancellation token.
    /// Used by the subscription root's <see cref="IAsyncEnumerable{OrderDto}"/>
    /// resolvers. Allocation-light: no buffer, no intermediate list.
    /// </summary>
    private static async IAsyncEnumerable<OrderDto> MapToDtosAsync(
        IAsyncEnumerable<OrderInfo> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var info in source.WithCancellation(cancellationToken))
        {
            yield return ToDto(info);
        }
    }
}

/// <summary>
/// Minimal <see cref="IObservable{T}"/> adapter that pumps values from an
/// <see cref="IAsyncEnumerable{T}"/>. Used by <c>OrderAdapter</c>'s
/// subscription root to satisfy <see cref="GraphQL.Resolvers.SourceStreamResolver{T}"/>'s
/// <c>IObservable</c>-typed delegate while still sourcing events from the
/// project's natural <see cref="IAsyncEnumerable{T}"/> pipeline (no
/// <c>System.Reactive</c> package dependency). See Plan C §C.3.3.
/// </summary>
file static class AsyncEnumerableObservable
{
    public static IObservable<T> ToObservable<T>(
        IAsyncEnumerable<T> source,
        CancellationToken cancellationToken)
    {
        return new EnumerableObservable<T>(source, cancellationToken);
    }

    private sealed class EnumerableObservable<T> : IObservable<T>
    {
        private readonly IAsyncEnumerable<T> _source;
        private readonly CancellationToken _cancellationToken;

        public EnumerableObservable(IAsyncEnumerable<T> source, CancellationToken cancellationToken)
        {
            _source = source;
            _cancellationToken = cancellationToken;
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            // One pump per subscription. Unsubscribe by completing or by
            // signalling an error; we don't expose an explicit cancellation
            // token to the observer because the upstream cancellation
            // token is sufficient for the GraphQL subscription lifetime.
            _ = PumpAsync(observer, _cancellationToken);
            return new Subscription();
        }

        private async Task PumpAsync(IObserver<T> observer, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var item in _source.WithCancellation(cancellationToken)
                                                   .ConfigureAwait(false))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    observer.OnNext(item);
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    observer.OnCompleted();
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Quiet: upstream cancellation. Don't call OnCompleted after cancel.
            }
            catch (Exception ex)
            {
                observer.OnError(ex);
            }
        }

        private sealed class Subscription : IDisposable
        {
            public void Dispose()
            {
                // No-op: pump is owned by the GraphQL subscription
                // lifetime, which cancels via the upstream
                // CancellationToken when the client disconnects.
            }
        }
    }
}
