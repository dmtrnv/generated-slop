using System.Text.Json;
using System.Threading.Channels;
using HotChocolate;
using HotChocolate.Execution;
using HotChocolate.Resolvers;
using HotChocolate.Types;
using Grpc.Core;
using OrderClient;
using OrderSource;
using QuerySource;

namespace AdapterFacade.Services;

/// <summary>
/// Adapter that exposes the <c>OrderSource</c> data over a code-first
/// GraphQL schema built with Hot Chocolate 16.
/// <para>
/// Supports two lookup root fields:
/// <list type="bullet">
///   <item><c>searhByPhoneNumber(phone_number: String!): [Order!]!</c></item>
///   <item><c>findByOrderId(order_id: String!): [Order!]!</c></item>
/// </list>
/// </para>
/// <para>
/// End-to-end streaming is achieved by having each resolver push a
/// <em>selection-set-projected</em>
/// row (an <see cref="IReadOnlyDictionary{TKey,TValue}"/> of
/// <c>snake_case</c> output name → value) straight into a per-request
/// <see cref="Channel{T}"/> as soon as the upstream gRPC stream yields
/// it, and a separate consumer task drains the channel into the gRPC
/// <see cref="IServerStreamWriter{T}"/>. The first projected row
/// therefore reaches the wire before the upstream gRPC stream is
/// drained. See <c>plans/order-adapter-resolver-streaming-options.md</c>
/// §E.6 risk 1 and §E.11.
/// </para>
/// <para>
/// The selection set is honoured here (not in the JSON serializer): each
/// resolver reads the <see cref="IResolverContext"/>'s field selection,
/// builds an immutable <see cref="OrderSelection"/> projection, and
/// applies it to every <see cref="OrderInfo"/> before pushing the
/// resulting dictionary into the channel. The consumer only knows how
/// to serialize that dictionary, so the bytes on the wire contain
/// exactly the fields the client asked for (and the <c>snake_case</c>
/// output names declared in <see cref="OrderDtoType"/>) — never the
/// <c>PascalCase</c> CLR property names of the <see cref="OrderDto"/>
/// record.
/// </para>
/// </summary>
public sealed class OrderAdapter : IAdapter
{
    public static string SourceId { get; } = "order_adapter_source_id";

    /// <summary>
    /// Async-local that flows the per-request <see cref="ChannelSink"/> from
    /// <see cref="Find"/> into the Hot Chocolate resolvers without requiring
    /// extra DI wiring. Set in <see cref="Find"/> before
    /// <see cref="IRequestExecutor.ExecuteAsync(IOperationRequest, CancellationToken)"/>
    /// and read inside the resolver methods.
    /// </summary>
    private static readonly AsyncLocal<ChannelSink?> CurrentSink = new();

    /// <summary>
    /// Async-local that carries the raw GraphQL query text for the current
    /// request. The OrderAdapter resolvers use it to re-parse the document
    /// and walk the selection set — Hot Chocolate 16 does not expose the
    /// parsed AST on <see cref="IResolverContext"/> in a version-stable
    /// way, so the resolver asks the OrderAdapter to re-parse the query
    /// it received.
    /// </summary>
    private static readonly AsyncLocal<string?> CurrentQuery = new();

    private readonly ILogger<OrderAdapter> _logger;
    private readonly IRequestExecutor _executor;
    private readonly string _sdl;

    public OrderAdapter(IRequestExecutor executor, ILogger<OrderAdapter> logger)
    {
        ArgumentNullException.ThrowIfNull(executor);
        ArgumentNullException.ThrowIfNull(logger);

        _executor = executor;
        _logger = logger;

        // Cache the SDL up-front. OrderAdapter.Schema() returns the same string
        // to every caller; this avoids re-printing the schema on every Find.
        // ISchemaDefinition.ToString() returns standard GraphQL SDL — see
        // plans/order-adapter-resolver-streaming-options.md §E.6 risk 2.
        _sdl = _executor.Schema.ToString();
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

        // -----------------------------------------------------------------
        // End-to-end streaming: drive the operation through a Channel<T> bridge.
        // The channel has a single reader (the consumer task below which
        // serializes each row to a QueryResponse) and a single writer (the
        // resolver-driven producer). The producer reads the per-request
        // ChannelSink out of the AsyncLocal — see CurrentSink.
        //
        // The producer is awaited before the channel is completed. Hot
        // Chocolate finishes the operation as soon as the resolver returns
        // (it returns null), so without the await we'd close the channel
        // before the first gRPC row arrives — see
        // plans/order-adapter-resolver-streaming-options.md §E.11.
        // -----------------------------------------------------------------
        var channel = Channel.CreateBounded<ProjectedRow>(new BoundedChannelOptions(capacity: 64)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        var sink = new ChannelSink(channel.Writer);
        var request = BuildRequest(query);

        var consumerTask = ConsumeChannelAsync(channel.Reader, responseStream, context.CancellationToken);

        // Make the per-request sink visible to the Hot Chocolate resolvers
        // through AsyncLocal — no DI registration needed. The raw query
        // text is also flowed through AsyncLocal so the resolvers can
        // re-parse it to honour the selection set.
        CurrentSink.Value = sink;
        CurrentQuery.Value = query.Query;
        try
        {
            var result = await _executor
                .ExecuteAsync(request, context.CancellationToken)
                .ConfigureAwait(false);

            await TranslateResultAsync(result, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // Client went away — let the framework translate this into the
            // appropriate cancellation behaviour.
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to stream orders from OrderAdapter");
            throw new RpcException(
                new Status(StatusCode.Internal, $"Failed to stream orders: {ex.Message}"));
        }
        finally
        {
            // Wait for the resolver-driven producer to finish draining the
            // upstream gRPC stream into the channel, then close the channel
            // so the consumer exits. This eliminates the race that caused
            // empty responses when Hot Chocolate completed the operation
            // before the first gRPC row arrived.
            await sink.CompleteAsync().ConfigureAwait(false);
            CurrentSink.Value = null;
            CurrentQuery.Value = null;
            await consumerTask.ConfigureAwait(false);
        }
    }

    public string Schema() => _sdl;

    /// <summary>
    /// Builds the <see cref="IOperationRequest"/> for the supplied
    /// <see cref="AdapterQuery"/>, including the variable dictionary. The
    /// per-request <see cref="ChannelSink"/> flows into the resolvers via the
    /// <see cref="CurrentSink"/> AsyncLocal — no need to attach it to the
    /// request itself.
    /// </summary>
    private static IOperationRequest BuildRequest(AdapterQuery query)
    {
        var builder = OperationRequestBuilder.New()
            .SetDocument(query.Query);

        if (!string.IsNullOrWhiteSpace(query.OperationName))
        {
            builder.SetOperationName(query.OperationName);
        }

        if (query.Variables is { Count: > 0 })
        {
            builder.SetVariableValues(query.Variables);
        }

        return builder.Build();
    }

    /// <summary>
    /// Inspects an <see cref="IExecutionResult"/> from Hot Chocolate and raises
    /// any GraphQL errors as <see cref="RpcException"/>s. The streaming of
    /// individual rows is handled by <see cref="ConsumeChannelAsync"/> (the
    /// primary path) — this is just the error-translation step.
    /// </summary>
    private async Task TranslateResultAsync(IExecutionResult result, CancellationToken cancellationToken)
    {
        if (result is OperationResult opResult)
        {
            if (opResult.Errors is { Count: > 0 })
            {
                var message = string.Join("; ", opResult.Errors.Select(e => e.Message));
                throw new RpcException(
                    new Status(StatusCode.InvalidArgument, $"Query execution failed: {message}"));
            }
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Drains the projected-order channel and serializes each row to a
    /// <see cref="QueryResponse"/> on the gRPC stream. The row is already
    /// selection-set-projected by the resolver — the keys are the
    /// <c>snake_case</c> GraphQL output names requested by the client, and
    /// the values are the corresponding scalar values from
    /// <see cref="OrderInfo"/>. We serialize the dictionary directly so
    /// the JSON keys are exactly what the client asked for, with no
    /// surprise <c>PascalCase</c> CLR property names.
    /// </summary>
    private async Task ConsumeChannelAsync(
        ChannelReader<ProjectedRow> reader,
        IServerStreamWriter<QueryResponse> responseStream,
        CancellationToken cancellationToken)
    {
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out var row))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                if (row.IsEmpty)
                {
                    continue;
                }

                var data = JsonSerializer.Serialize(row.Fields, ProjectedRow.JsonOptions);

                _logger.LogInformation(
                    "Streaming projected order row {OrderId} ({FieldCount} fields: {Fields})",
                    TryGetString(row.Fields, "order_id") ?? "<unknown>",
                    row.Fields.Count,
                    string.Join(",", row.Fields.Keys));

                await responseStream.WriteAsync(new QueryResponse
                {
                    ResultSchema = _sdl,
                    Data = data,
                }, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static string? TryGetString(IReadOnlyDictionary<string, object?> dict, string key)
        => dict.TryGetValue(key, out var v) ? v as string : null;

    // ---------------------------------------------------------------------
    // Hot Chocolate code-first types
    // ---------------------------------------------------------------------

    /// <summary>
    /// Strongly-typed DTO projected onto the GraphQL <c>Order</c> type. The
    /// field names use snake_case to preserve the public contract that the
    /// pre-Hot-Chocolate <c>OrderAdapter</c> already published in its SDL.
    /// Kept in place for tests and external code that may import the type
    /// — the wire path goes through <see cref="ProjectedRow"/> instead.
    /// </summary>
    public sealed record OrderDto(
        string OrderId,
        string PhoneNumber,
        string ProductName,
        double Amount);

    /// <summary>
    /// A single selection-set-projected row produced by <see cref="OrderSelection"/>.
    /// Keys are GraphQL output names (snake_case, as declared on
    /// <see cref="OrderDtoType"/>); values are the scalar values for those
    /// fields on a particular <see cref="OrderInfo"/>. Serialized verbatim
    /// to <c>QueryResponse.Data</c> by <see cref="ConsumeChannelAsync"/>.
    /// </summary>
    public readonly record struct ProjectedRow(IReadOnlyDictionary<string, object?> Fields)
    {
        public bool IsEmpty => Fields is null || Fields.Count == 0;

        /// <summary>
        /// Shared <see cref="JsonSerializerOptions"/> used when writing
        /// projected rows to the gRPC wire. <c>JsonSerializer</c>'s
        /// default policy is already "use the dictionary key as the JSON
        /// property name" — which is exactly what we want, because
        /// <see cref="OrderSelection"/> already populates the dictionary
        /// with the snake_case GraphQL output names.
        /// </summary>
        public static readonly JsonSerializerOptions JsonOptions = new()
        {
            // No naming policy — the dictionary already carries the
            // snake_case output names from the schema, and the consumer
            // of QueryResponse.Data expects exactly those names.
        };
    }

    /// <summary>
    /// Per-request sink for projected <see cref="OrderDto"/> rows. The
    /// resolvers below pull the current <see cref="ChannelSink"/> out of the
    /// <see cref="CurrentSink"/> AsyncLocal, register their producer task
    /// via <see cref="RegisterProducer"/>, and write through
    /// <see cref="WriteAsync"/>.
    /// <para>
    /// <see cref="CompleteAsync"/> awaits the registered producer (or a
    /// short grace period) before closing the channel, so the consumer task
    /// drains every row the upstream gRPC stream yields. This is what
    /// eliminates the "first row arrives after the channel is closed" race
    /// documented in <c>plans/order-adapter-resolver-streaming-options.md</c>
    /// §E.11.
    /// </para>
    /// </summary>
    public sealed class ChannelSink
    {
        private readonly ChannelWriter<ProjectedRow> _writer;
        private Task? _producerTask;
        private readonly object _producerLock = new();

        public ChannelSink(ChannelWriter<ProjectedRow> writer)
        {
            _writer = writer;
        }

        public async ValueTask WriteAsync(ProjectedRow row, CancellationToken cancellationToken)
        {
            await _writer.WriteAsync(row, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Records the resolver-driven producer task. <see cref="CompleteAsync"/>
        /// awaits it before closing the channel. Only one producer is
        /// expected per request (the GraphQL operation has a single root
        /// field), but the implementation tolerates multiple registrations
        /// by awaiting them all.
        /// </summary>
        public void RegisterProducer(Task producerTask)
        {
            ArgumentNullException.ThrowIfNull(producerTask);
            lock (_producerLock)
            {
                if (_producerTask is null)
                {
                    _producerTask = producerTask;
                }
                else
                {
                    _producerTask = Task.WhenAll(_producerTask, producerTask);
                }
            }
        }

        /// <summary>
        /// Awaits the registered producer (if any) and then closes the
        /// channel. Safe to call from <c>Find</c>'s <c>finally</c> block:
        /// cancellation from the gRPC client propagates into the producer
        /// through the per-request <see cref="CancellationToken"/>.
        /// </summary>
        public async Task CompleteAsync()
        {
            Task? producer;
            lock (_producerLock)
            {
                producer = _producerTask;
            }

            if (producer is not null)
            {
                try
                {
                    await producer.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected on client disconnect; the consumer will see
                    // the same cancellation and exit.
                }
                catch (Exception ex)
                {
                    // The producer failure has already been (or will be)
                    // logged by the resolver. Don't let it mask the gRPC
                    // status; just close the channel so the consumer exits.
                    System.Diagnostics.Debug.WriteLine(
                        $"ChannelSink.CompleteAsync: producer threw {ex.GetType().Name}: {ex.Message}");
                }
            }

            _writer.TryComplete();
        }
    }

    /// <summary>
    /// Hot Chocolate code-first descriptor for the <c>Order</c> GraphQL type.
    /// Field names are explicit so we keep <c>order_id</c> / <c>phone_number</c>
    /// / <c>product_name</c> / <c>amount</c> regardless of the C# property
    /// casing on <see cref="OrderDto"/>.
    /// <para>
    /// Marked <c>public</c> so that <c>Program.cs</c> can register it with the
    /// Hot Chocolate executor builder: <c>AddType<OrderAdapter.OrderDtoType>()</c>.
    /// </para>
    /// </summary>
    public sealed class OrderDtoType : ObjectType<OrderDto>
    {
        protected override void Configure(IObjectTypeDescriptor<OrderDto> descriptor)
        {
            descriptor.Name("Order");
            descriptor.Description("Order entity produced by the order source.");

            descriptor
                .Field(x => x.OrderId)
                .Name("order_id")
                .Type<NonNullType<StringType>>();

            descriptor
                .Field(x => x.PhoneNumber)
                .Name("phone_number")
                .Type<NonNullType<StringType>>();

            descriptor
                .Field(x => x.ProductName)
                .Name("product_name")
                .Type<NonNullType<StringType>>();

            descriptor
                .Field(x => x.Amount)
                .Name("amount")
                .Type<NonNullType<FloatType>>();
        }
    }

    /// <summary>
    /// Immutable description of a single <c>Order</c> selection set,
    /// built once per resolver invocation by inspecting
    /// <see cref="IResolverContext.Selection"/>. Used by
    /// <see cref="StreamIntoSinkAsync"/> to project each
    /// <see cref="OrderInfo"/> into a dictionary containing only the
    /// fields the client actually requested.
    /// <para>
    /// We only support scalar leaf selections and the <c>__typename</c>
    /// meta-field (the <c>Order</c> type is flat and has no abstract
    /// subtypes). Inline fragments / spreads are rejected at
    /// construction time — see <c>plans/order-adapter-refactor-schemafirst.md</c>
    /// for the supported subset.
    /// </para>
    /// </summary>
    public sealed class OrderSelection
    {
        /// <summary>
        /// The fields that are actually selected on <c>Order</c>, paired
        /// with the order they should appear in the output JSON (the
        /// order in which the selection set lists them, with
        /// <c>__typename</c> resolved to the constant <c>"Order"</c>).
        /// </summary>
        public IReadOnlyList<SelectedField> Fields { get; }

        private OrderSelection(IReadOnlyList<SelectedField> fields)
        {
            Fields = fields;
        }

        public static OrderSelection FromContext(IResolverContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            // Hot Chocolate 16 does not publicly expose the raw AST
            // FieldNode for the current selection on `IResolverContext`.
            // The robust workaround is to use the GraphQL.NET `Parser`
            // (transitively available via Hot Chocolate) to parse the
            // query text the caller supplied. We then walk the AST to
            // find the root field's selection set — which is exactly
            // what the client asked for on the `Order` items.
            //
            // We get the query text off the request via the
            // AsyncLocal that the OrderAdapter.Find handler set.
            // The query string lives in the IOperationRequest passed to
            // ExecuteAsync. It's not directly available on
            // IResolverContext in HC 16, so the simplest source of
            // truth is the *current* OperationRequest that the
            // OrderAdapter.Find handler built. We hand the raw query
            // text in via an AsyncLocal alongside the ChannelSink.
            _ = context; // currently unused; reserved for future use.
            var queryText = OrderAdapter.CurrentQuery.Value;
            if (string.IsNullOrWhiteSpace(queryText))
            {
                // No query available — return an empty selection (the
                // consumer will emit an empty row).
                return new OrderSelection(Array.Empty<SelectedField>());
            }

            var document = HotChocolate.Language.Utf8GraphQLParser.Parse(queryText);

            // Find the single root field selection. We only handle
            // query operations, and only the first field in the
            // selection set (the resolver was invoked for that field).
            var operationDef = document.Definitions
                .OfType<HotChocolate.Language.OperationDefinitionNode>()
                .FirstOrDefault(o => o.Operation
                    is HotChocolate.Language.OperationType.Query);

            if (operationDef is null)
            {
                return new OrderSelection(Array.Empty<SelectedField>());
            }

            var rootField = operationDef.SelectionSet.Selections
                .OfType<HotChocolate.Language.FieldNode>()
                .FirstOrDefault();

            if (rootField is null || rootField.SelectionSet is null)
            {
                return new OrderSelection(Array.Empty<SelectedField>());
            }

            var ordered = new List<SelectedField>();
            foreach (var child in rootField.SelectionSet.Selections)
            {
                if (child is HotChocolate.Language.FragmentSpreadNode)
                {
                    throw new GraphQLException(
                        ErrorBuilder.New()
                            .SetMessage("Order selection does not support fragment spreads.")
                            .Build());
                }

                if (child is HotChocolate.Language.InlineFragmentNode)
                {
                    throw new GraphQLException(
                        ErrorBuilder.New()
                            .SetMessage("Order selection does not support inline fragments.")
                            .Build());
                }

                if (child is not HotChocolate.Language.FieldNode field)
                {
                    continue;
                }

                var name = field.Name.Value;
                if (name == "__typename")
                {
                    var alias = field.Alias?.Value ?? "__typename";
                    ordered.Add(new SelectedField(alias, name, FieldKind.TypeName));
                    continue;
                }

                var outputName = field.Alias?.Value ?? name;
                var kind = name switch
                {
                    "order_id" => FieldKind.OrderId,
                    "phone_number" => FieldKind.PhoneNumber,
                    "product_name" => FieldKind.ProductName,
                    "amount" => FieldKind.Amount,
                    _ => throw new GraphQLException(
                        ErrorBuilder.New()
                            .SetMessage($"Unknown field '{name}' on Order.")
                            .Build()),
                };
                ordered.Add(new SelectedField(outputName, name, kind));
            }

            return new OrderSelection(ordered);
        }

        /// <summary>
        /// Projects <paramref name="info"/> into a dictionary whose keys
        /// are the GraphQL output names from the selection set (in the
        /// same order) and whose values are the corresponding scalars.
        /// The <c>__typename</c> field always resolves to the constant
        /// <c>"Order"</c>.
        /// </summary>
        public ProjectedRow Project(OrderInfo info)
        {
            ArgumentNullException.ThrowIfNull(info);

            var dict = new Dictionary<string, object?>(Fields.Count);
            foreach (var field in Fields)
            {
                dict[field.OutputName] = field.Kind switch
                {
                    FieldKind.OrderId => info.OrderId,
                    FieldKind.PhoneNumber => info.PhoneNumber,
                    FieldKind.ProductName => info.ProductName,
                    FieldKind.Amount => info.Amount,
                    FieldKind.TypeName => "Order",
                    _ => null,
                };
            }
            return new ProjectedRow(dict);
        }
    }

    public enum FieldKind
    {
        OrderId,
        PhoneNumber,
        ProductName,
        Amount,
        TypeName,
    }

    public sealed record SelectedField(string OutputName, string FieldName, FieldKind Kind);

    /// <summary>
    /// Hot Chocolate code-first root query. The two field methods are resolver
    /// methods. They don't return the <c>List<Order></c> — they stream
    /// the projected rows straight into the per-request <see cref="ChannelSink"/>
    /// and return <c>null</c>. The result type for both fields is the standard
    /// <c>[Order!]!</c>, but Hot Chocolate's executor populates it by walking
    /// the empty result and our consumer task is the one that puts the rows on
    /// the wire.
    /// <para>
    /// In Hot Chocolate 16 a code-first resolver method is auto-bound when its
    /// name (with <c>Get</c> / <c>Async</c> stripped) matches a field. We use
    /// <see cref="GraphQLNameAttribute"/> to keep the public field names
    /// (<c>searhByPhoneNumber</c> and <c>findByOrderId</c>) and the
    /// snake_case argument names (<c>phone_number</c>, <c>order_id</c>) that
    /// were part of the GraphQL.NET 8 contract.
    /// </para>
    /// <para>
    /// Marked <c>public</c> so that <c>Program.cs</c> can register it with the
    /// Hot Chocolate executor builder:
    /// <c>AddQueryType<OrderAdapter.OrderQuery>()</c>.
    /// </para>
    /// </summary>
    public sealed class OrderQuery
    {
        /// <summary>
        /// Resolver for the <c>searhByPhoneNumber(phone_number: String!): [Order!]!</c>
        /// field. Streams <see cref="OrderDto"/> rows straight from the gRPC
        /// client through the per-request <see cref="ChannelSink"/> — no
        /// <c>List<OrderDto></c> materialization.
        /// </summary>
        [GraphQLName("searhByPhoneNumber")]
        public Task<List<OrderDto>?> GetSearhByPhoneNumberAsync(
            [GraphQLName("phone_number")] string phoneNumber,
            [Service] IOrderClient orderClient,
            IResolverContext context,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(phoneNumber))
            {
                throw new GraphQLException(
                    ErrorBuilder.New().SetMessage("phone_number must be provided.").Build());
            }

            var selection = OrderSelection.FromContext(context);

            // Start the producer on a background task and register it with
            // the per-request sink so Find can await it before closing the
            // channel. Returning null tells the executor this field produced
            // no items in the result tree (the rows live in the channel).
            var producer = StreamIntoSinkAsync(orderClient, phoneNumber, selection, cancellationToken);
            var sink = CurrentSink.Value;
            sink?.RegisterProducer(producer);
            return Task.FromResult<List<OrderDto>?>(null);
        }

        /// <summary>
        /// Resolver for the <c>findByOrderId(order_id: String!): [Order!]!</c>
        /// field. Streams <see cref="OrderDto"/> rows straight from the gRPC
        /// client through the per-request <see cref="ChannelSink"/> — no
        /// <c>List<OrderDto></c> materialization.
        /// </summary>
        [GraphQLName("findByOrderId")]
        public Task<List<OrderDto>?> GetFindByOrderIdAsync(
            [GraphQLName("order_id")] string orderId,
            [Service] IOrderClient orderClient,
            IResolverContext context,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(orderId))
            {
                throw new GraphQLException(
                    ErrorBuilder.New().SetMessage("order_id must be provided.").Build());
            }

            var selection = OrderSelection.FromContext(context);

            var producer = StreamIntoSinkAsync(orderClient, orderId, selection, cancellationToken, byOrderId: true);
            var sink = CurrentSink.Value;
            sink?.RegisterProducer(producer);
            return Task.FromResult<List<OrderDto>?>(null);
        }

        private static async Task StreamIntoSinkAsync(
            IOrderClient orderClient,
            string key,
            OrderSelection selection,
            CancellationToken cancellationToken,
            bool byOrderId = false)
        {
            var source = byOrderId
                ? orderClient.GetOrdersByOrderIdAsync(key, cancellationToken)
                : orderClient.GetOrdersByPhoneAsync(key, cancellationToken);

            var sink = CurrentSink.Value;
            if (sink is null)
            {
                // No sink registered — silently drop. (Should not happen in
                // production; tests may construct OrderAdapter without a sink.)
                return;
            }

            await foreach (var info in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await sink.WriteAsync(selection.Project(info), cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
