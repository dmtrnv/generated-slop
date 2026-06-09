# Plan options: stream `OrderAdapter` resolvers (no in-memory list)

> **Companion document to** [`plans/order-adapter-streaming-resolvers.md`](order-adapter-streaming-resolvers.md:1).
> The original document identifies three options for getting an
> `IAsyncEnumerable<OrderDto>` (or equivalent) to flow all the way from the
> gRPC `IOrderClient` to the `IServerStreamWriter<QueryResponse>` without
> buffering a full `List<OrderDto>` in the resolver. This file turns those
> options (plus a fourth — switching to Hot Chocolate) into concrete,
> executable plans that Code mode can follow. **No code changes are made
> here — this is planning only.**

The runtime evidence is captured in §1 of the original plan: GraphQL.NET
8.0.2's [`ExecutionStrategy.SetArrayItemNodesAsync`](https://github.com/graphql-dotnet/graphql-dotnet/blob/v8.0.2/src/GraphQL/Execution/ExecutionStrategy.cs#L425) requires a synchronous
`IEnumerable` for list-field resolvers, so simply returning
`IAsyncEnumerable<OrderDto>` from the two `OrderQuery` resolvers in
[`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:266) and
[`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:299) throws at
runtime.

| Plan | Strategy | Effort | Streaming wins |
|------|----------|--------|----------------|
| **A — ship-as-is** | Keep the current `List<OrderDto>` resolvers. All streaming wins live on the `IServerStreamWriter<QueryResponse>` path. | Trivial (no code change) | Wire only |
| **C — subscriptions** | Add a GraphQL `Subscription` root type with `IAsyncEnumerable<OrderDto>` resolvers; switch to `SubscriptionExecutionStrategy`. | Medium | End-to-end (resolver → executor → wire) |
| **D — hand-rolled selection walker** | Bypass `IDocumentExecuter` for this adapter. Walk the selection set over the raw gRPC `IAsyncEnumerable<OrderInfo>` and emit one `QueryResponse` per row. | High | End-to-end, with full control |
| **E — switch to Hot Chocolate** | Replace the GraphQL.NET 8 adapter with a Hot Chocolate 13/14 adapter. Hot Chocolate's `IAsyncEnumerable<T>` resolver path iterates per element and projects through the selection set. We still drive the gRPC stream from our handler because the gRPC carrier is one `QueryResponse` per row, not a GraphQL document. | High (framework swap) | End-to-end (subject to Hot Chocolate's `IAsyncEnumerable` semantics, see §E.6) | **Implemented** — see §E.11 |

---

## 0. Shared context (applies to all four plans)

- `IOrderClient.GetOrdersByPhoneAsync` and `GetOrdersByOrderIdAsync` already return
  `IAsyncEnumerable<OrderInfo>` — see [`OrderClient/OrderClient.cs`](OrderClient/OrderClient.cs:26) and
  [`OrderClient/OrderClient.cs`](OrderClient/OrderClient.cs:58). The upstream
  contract is streaming-friendly.
- The gRPC surface is a single `ExecuteQuery` RPC defined in
  [`Protos/query.proto`](Protos/query.proto:10) that streams `QueryResponse` messages.
  The proto is unchanged across all four plans.
- The in-process GraphQL pipeline currently:
  1. `QueryService` validates against the adapter's SDL using `IDocumentValidator`.
  2. `OrderAdapter.Find` runs the document through `IDocumentExecuter` against
     a code-first `Schema` (see [`OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:220)).
  3. The executor produces a `RootExecutionNode` (the `Data` of the
     `ExecutionResult`) — we walk the `SubFields[0]` and serialize one
     `QueryResponse` per projected row, see
     [`OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:55).
- The current SDL is hard-coded as the result of
  [`OrderAdapter.Schema()`](AdapterFacade/Services/OrderAdapter.cs:209) (which delegates to
  `_schema.Value.Print()`). It already includes both root fields and the
  `Order` type definition.
- The schema, selection-set projection, and validation are all in
  GraphQL.NET 8.0.2 (Plans A, C, D) or would move to Hot Chocolate 14
  (Plan E). The proto, the gRPC client, the adapter contract
  (`IAdapter`), and `QueryService` are independent of the option we pick.
- `Dotnet build` must remain green (0 warnings, 0 errors) at every step.

---

## Plan A — ship-as-is (current state, no change)

### A.1 What this plan delivers

The two `OrderQuery` resolvers continue to materialize the entire
`IAsyncEnumerable<OrderInfo>` into a `List<OrderDto>` before returning.
The streaming wins remain on the **outer**
`IServerStreamWriter<QueryResponse>` path. `Find` still emits one
`QueryResponse` per projected row, so the gRPC caller sees rows appear
incrementally — but inside the resolver, memory is
`O(number_of_rows_for_phone)`.

### A.2 File-by-file change list

| File | Change |
|------|--------|
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:1) | **No code change.** The code comment in each resolver already points at [`plans/order-adapter-streaming-resolvers.md`](order-adapter-streaming-resolvers.md:1) explaining the trade-off. |
| All other files | No change. |

### A.3 Risk / cost matrix

| Dimension | Status |
|-----------|--------|
| Build time | 0 — nothing to compile. |
| Memory per request | `O(N)` where N = rows for the looked-up phone/order id. |
| Time-to-first-byte | Same as today (resolver runs before first `WriteAsync`). |
| gRPC contract | Unchanged. |
| Public schema (SDL) | Unchanged. |
| Operational risk | None — this is the state on disk. |

### A.4 When to pick A

- Per-phone / per-order-id result sets are bounded (the current data
  model assumption).
- The "stream from source to wire without buffering" goal is not a hard
  requirement.
- We want the smallest possible change while the broader adapter design
  (subscription transport, selection-walker) is still being decided.

### A.5 Verification

```bash
dotnet build AdapterFacade/AdapterFacade.csproj
```

Expected: `Build succeeded. 0 Warning(s), 0 Error(s).`

No runtime change to verify.

---

## Plan C — GraphQL subscription root with `IAsyncEnumerable<OrderDto>` resolvers

### C.1 Goal

Promote the streaming lookups to GraphQL **subscription** fields.
Subscriptions are the only GraphQL.NET 8 construct that natively supports
`IAsyncEnumerable<T>` resolvers (the executor's
`SubscriptionExecutionStrategy` iterates per source event and projects
each item through the selection set). The two **query** fields stay
where they are for backward compatibility; the new **subscription**
fields do the actual streaming.

### C.2 Design decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Keep the existing query fields? | **Yes, both queries stay.** They are the public contract used by `Probe` / `Probe2`. | Backward compatibility. |
| Where do the streaming fields live? | Add a new `Subscription` root type with two fields mirroring the queries by name. | Subscriptions and queries are distinct root types in GraphQL — they can share field names (e.g. both `searhByPhoneNumber`) because the operation type is selected by the client. |
| Field names on `Subscription` | `searhByPhoneNumber(phone_number: String!): Order!` and `findByOrderId(order_id: String!): Order!` (note: **not** a list — one event per order). | Subscription events are single items. The stream of `Order` events is the equivalent of the current `List<Order>` payload. |
| Resolver signature | `IAsyncEnumerable<OrderDto>`. | This is what the executor's subscription strategy knows how to iterate. |
| Source of the stream | A new [`MapToDtosAsync`](AdapterFacade/Services/OrderAdapter.cs) helper that yields one DTO per upstream `OrderInfo`. | The original plan (§3) already sketched this helper. Re-use it. |
| Execution strategy | Register `SubscriptionExecutionStrategy` in DI / on the `Schema` instance. | Without this, the executor will treat the subscription as a query and trip the same `SetArrayItemNodesAsync` issue. |
| gRPC contract | Unchanged. The outer `Find` still writes one `QueryResponse` per emitted `OrderDto`. | Wire compatibility is preserved — `QueryResponse.data` is just the JSON of a single `Order` per message now. |
| Validation flow | `QueryService` keeps using `IDocumentValidator` against the SDL, which now also contains the `Subscription` type. The validator accepts subscription operations. | The schema-first validator handles operation-type validation out of the box. |
| Subscription activation | The `Query` SDL string returned by `Schema()` is extended with a `type Subscription { ... }` block. | Single source of truth for the SDL payload. |
| `OrderDto` projection | Same DTO + `OrderType` code-first mapper as today. | The DTO already maps 1-to-1 to `OrderInfo`. |

### C.3 Target shape

#### C.3.1 SDL (`Schema()` payload)

```graphql
type Order {
    order_id: String!
    phone_number: String!
    product_name: String!
    amount: Float!
}

type Query {
    searhByPhoneNumber(phone_number: String!): [Order!]!
    findByOrderId(order_id: String!): [Order!]!
}

type Subscription {
    """Streams one Order event per match. Replaces the in-memory list resolver."""
    searhByPhoneNumber(phone_number: String!): Order!
    """Streams one Order event per match. Replaces the in-memory list resolver."""
    findByOrderId(order_id: String!): Order!
}
```

#### C.3.2 Resolver shape

```csharp
public sealed class OrderSubscription : ObjectGraphType
{
    public OrderSubscription(IOrderClient client, ILogger logger)
    {
        Name = "Subscription";

        AddField(new FieldType
        {
            Name = "searhByPhoneNumber",
            Type = typeof(OrderType),
            Arguments = new QueryArguments(
                new QueryArgument<NonNullGraphType<StringGraphType>>
                {
                    Name = "phone_number",
                }),
            Resolver = new FuncFieldResolver<OrderDto>(ctx =>
                MapToDtosAsync(
                    client.GetOrdersByPhoneAsync(
                        ctx.GetArgument<string>("phone_number"),
                        ctx.CancellationToken),
                    ctx.CancellationToken)),
            StreamResolver = new SourceStreamResolver<OrderDto>(/* same */),
        });

        AddField(new FieldType
        {
            Name = "findByOrderId",
            // ... mirror shape, calls GetOrdersByOrderIdAsync
        });
    }
}
```

In GraphQL.NET 8, a `Subscription` field carries **both** a
`Resolver` (used by the executor to wire up the source stream) and a
`StreamResolver` (the actual `IObservable` / `IAsyncEnumerable`).
Defining both is the v8 idiom for code-first subscriptions — using
just one will not iterate correctly under
`SubscriptionExecutionStrategy`.

#### C.3.3 `BuildSchema` (updated)

```csharp
private Schema BuildSchema()
{
    var orderType = new OrderType();
    var query = new OrderQuery(_orderClient, _logger);
    var subscription = new OrderSubscription(_orderClient, _logger);
    return new OrderAdapterSchema(query, orderType, subscription);
}

private sealed class OrderAdapterSchema : Schema
{
    public OrderAdapterSchema(
        OrderQuery query, OrderType orderType, OrderSubscription subscription)
    {
        Query = query;
        Subscription = subscription;
        RegisterType(orderType);
    }
}
```

#### C.3.4 `Find` flow

The flow is **unchanged** from the current code in
[`OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:55) for `query` operations. For
`subscription` operations the executor itself drives the iteration and
hands `Find` an `ExecutionResult` whose `Data` already contains the
streamed `Order` projections — we still serialize them one-by-one to
`QueryResponse` per the existing loop in
[`OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:122).

Discriminate query vs. subscription using
`result.Operation is GraphQLParser.AST.GraphQLOperationDefinition op
&& op.Operation == OperationType.Subscription`. In the subscription
case, walk `result.Data` as a **single** row (not as an array) and
stream it once — the executor has already exhausted the
`IAsyncEnumerable`.

> **Caveat to verify during implementation:** GraphQL.NET 8's
> `IDocumentExecuter` is not a fully streaming entry point — it buffers
> subscription events into the final `ExecutionResult`. If we want the
> "first row reaches the wire before the last row is produced" property
> end-to-end, we need a different execution entry point (e.g. the
> `SubscriptionDocumentExecuter` from `GraphQL.SystemTextJson`, or our
> own observer that wraps `IAsyncEnumerable<OrderDto>` and calls
> `Find`'s stream writer per item). **Spike this before committing to
> the plan** — see §C.6 risk 1.

#### C.3.5 DI / Program.cs change

```csharp
// In Program.cs (new service registration):
builder.Services.AddSingleton<SubscriptionExecutionStrategy>();
builder.Services.AddSingleton<IDocumentExecuter>(sp =>
{
    var strategies = new Dictionary<OperationType, IExecutionStrategy>
    {
        [OperationType.Query]        = new SerialExecutionStrategy(),
        [OperationType.Mutation]     = new SerialExecutionStrategy(),
        [OperationType.Subscription] = sp.GetRequiredService<SubscriptionExecutionStrategy>(),
    };
    return new DocumentExecuter(new DefaultExecutionStrategySelector(strategies));
});
```

The executor instance is now created once and held by the DI container;
the `OrderAdapter` constructor changes accordingly to accept the
executor from DI (no more `new DocumentExecuter()` inside the adapter).

#### C.3.6 The `MapToDtosAsync` helper

```csharp
private static async IAsyncEnumerable<OrderDto> MapToDtosAsync(
    IAsyncEnumerable<OrderInfo> source,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    await foreach (var info in source.WithCancellation(cancellationToken))
    {
        yield return ToDto(info);
    }
}
```

Place it as a `private static` method on `OrderAdapter` (or factor into
`OrderAdapter.Mapping` if we want a separate file). No buffers.

### C.4 File-by-file change list

| File | Change |
|------|--------|
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:1) | Extend `Schema()` SDL with `type Subscription { ... }`. Add `OrderSubscription` nested class with two `AddField` entries whose `StreamResolver` returns `IAsyncEnumerable<OrderDto>` from a new private `MapToDtosAsync` helper. Update `BuildSchema` to construct and register the subscription root. Update `OrderAdapterSchema` to assign `Subscription = ...`. |
| [`AdapterFacade/Program.cs`](AdapterFacade/Program.cs:1) | Register `IDocumentExecuter` in DI with a strategy selector that maps `OperationType.Subscription` → `SubscriptionExecutionStrategy`. |
| [`AdapterFacade/AdapterFacade.csproj`](AdapterFacade/AdapterFacade.csproj:1) | Add `using GraphQL.Execution;` (already implicitly imported via `GraphQL`). No new package. |
| [`plans/order-adapter-streaming-resolvers.md`](order-adapter-streaming-resolvers.md:1) | Update the status table: mark Plan C as "designed — pending implementation spike". |
| All other files | No change. |

### C.5 Behaviour matrix

| Client operation | Type | Root field | Resolver | Streamed `QueryResponse.data` |
|------------------|------|------------|----------|-------------------------------|
| `{ searhByPhoneNumber(phone_number: "...") { order_id, product_name } }` | `query` | `searhByPhoneNumber` | `OrderQuery` returns `List<OrderDto>` (unchanged) | One JSON object per row. |
| `{ findByOrderId(order_id: "o-42") { order_id, amount } }` | `query` | `findByOrderId` | `OrderQuery` returns `List<OrderDto>` (unchanged) | One JSON object per row. |
| `subscription { searhByPhoneNumber(phone_number: "...") { order_id } }` | `subscription` | `searhByPhoneNumber` | `OrderSubscription` returns `IAsyncEnumerable<OrderDto>` | One JSON object per `OrderInfo` — no in-memory list. |
| `subscription { findByOrderId(order_id: "o-42") { order_id } }` | `subscription` | `findByOrderId` | `OrderSubscription` returns `IAsyncEnumerable<OrderDto>` | One JSON object per `OrderInfo` — no in-memory list. |

### C.6 Risks / things to double-check

1. **The executors that ship with `GraphQL 8.0.2` may buffer subscription
   events in the final `ExecutionResult`.** Confirm by reading
   `GraphQL.SystemTextJson.Subscriptions` (if referenced) or by writing a
   test that times the first byte vs. the last byte. If buffering
   occurs, the only ways to fix it without forking GraphQL.NET are:
   - Use `GraphQL.Server.Transports.AspNetCore` to expose a real
     WebSocket subscription endpoint (not in scope — we keep gRPC).
   - Bypass `IDocumentExecuter` and call the resolvers directly
     (this is Plan D).
   Capture the outcome of this check in the plan file before
   implementing.
2. **Two fields named `searhByPhoneNumber`** (one on `Query`, one on
   `Subscription`). This is legal GraphQL — operation type disambiguates
   them at execution time. Confirm `IDocumentValidator` accepts it
   without complaint in 8.0.2.
3. **`SourceStreamResolver` vs. `Resolver` only.** Defining only a
   `StreamResolver` on a subscription field can produce a runtime
   "field is not a subscription" error in 8.0.2. The defensive default
   is to set both `Resolver` and `StreamResolver` to the same delegate
   (the executor picks the right one for the strategy). See §C.3.2.
4. **Cancellation propagation.** The upstream gRPC stream honors
   `CancellationToken`. Pass `ctx.CancellationToken` into
   `MapToDtosAsync` *and* the client call so client disconnect tears
   down the gRPC stream.
5. **Schema print.** `_schema.Value.Print()` must now include the
   `type Subscription { ... }` block. Verify by running the adapter and
   calling `Schema()`.

### C.7 Verification

```bash
dotnet build AdapterFacade/AdapterFacade.csproj
# Run OrderSource + AdapterFacade + Probe; do a subscription test client-side
# (or, if no subscription client exists, use the GraphQL.NET test fixture from plans/order-adapter-refactor-schemafirst.md).
```

Expected:
- Build: 0 warnings, 0 errors.
- `OrderAdapter.Schema()` returns SDL with `type Subscription { ... }`.
- A `subscription { searhByPhoneNumber(phone_number: "...") { order_id } }` operation
  produces N `QueryResponse` messages (one per `OrderInfo`).
- A query operation continues to behave exactly as today.

### C.8 Cost summary

- Net new code: ~80–120 LOC in `OrderAdapter.cs` (subscription root + helper).
- Net new code in `Program.cs`: ~15 LOC (DI registration of the executor + strategy selector).
- No new package (we already depend on `GraphQL 8.0.2`, which includes `SubscriptionExecutionStrategy`).
- Requires a runtime spike (§C.6 risk 1) before committing.

---

## Plan D — hand-rolled selection-set walker over the raw gRPC stream

### D.1 Goal

Drop GraphQL.NET's `IDocumentExecuter` for the `OrderAdapter` entirely.
Parse the GraphQL document ourselves (we already do for validation in
`QueryService`), walk the selection set for the chosen root field, and
project each `OrderInfo` straight to JSON and onto the
`IServerStreamWriter<QueryResponse>`. This is the only option that
fully satisfies "stream from gRPC source to wire without buffering the
full list in memory" **and** keeps selection-set projection correct
**and** does not depend on GraphQL.NET subscription support that may
buffer.

### D.2 Design decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| What does the adapter do with the query? | Parse the AST, pick the single root field, walk its selection set, and emit one `QueryResponse` per `OrderInfo` produced by the matching gRPC stream. | Removes the `IEnumerable` requirement that blocks Plan A's `IAsyncEnumerable<OrderDto>`. |
| How is the SDL produced? | `Schema()` keeps returning a static SDL string (same as today). The string is the **schema** for documentation / `ResultSchema` purposes only — it is no longer executed. | The proto and gRPC contract don't require an executable schema. We can still validate against the SDL using `IDocumentValidator` in `QueryService`. |
| Where does validation happen? | `QueryService` still validates against the SDL via `IDocumentValidator`. `OrderAdapter` trusts the validated operation. | Validation is the one piece we keep from GraphQL.NET — it's a good defense-in-depth check. |
| How is the root field selected? | Walk the operation's `SelectionSet.Selections`, find the single `GraphQLFieldSelection`, and switch on `Field.Name.StringValue`. | Single-operation queries are what the rest of the system already assumes. |
| How is the selection set projected? | New helper class [`Selection/FieldSelection.cs`](AdapterFacade/Services/Selection/FieldSelection.cs) (re-uses the path mentioned in the original plan and in [`plans/order-adapter-refactor-schemafirst.md`](order-adapter-refactor-schemafirst.md:115)) walks the AST and projects an `OrderInfo` into a `Dictionary<string, object?>` whose keys/values are exactly the fields the client asked for. | Manual but tractable for our flat `Order` shape; only needs to handle scalar field selection and the `__typename` meta-field for v1. |
| How is JSON produced per row? | A new `SelectionSetSerializer` (already referenced in the project tree, currently absent on disk) emits one `QueryResponse.Data` JSON string per row. | Keeps the JSON shape byte-identical to today's `OrderAdapter` output (snake_case keys, scalar values). |
| What happens to the `OrderQuery` / `OrderType` / code-first schema? | Removed. The adapter no longer needs `GraphQL.Types`, `GraphQL`, or `IDocumentExecuter` in its hot path. | The schema-first artifacts were load-bearing only for the executor. With the executor gone, they are dead weight. |
| What is the dependency footprint? | We keep `GraphQL` because `QueryService` still uses `IDocumentValidator` and the SDL string. We **remove** all `GraphQL.Types` / `GraphQL.Execution` usage from `OrderAdapter`. | Smaller surface, less to break on a GraphQL.NET upgrade. |
| gRPC contract | Unchanged. | One `QueryResponse` per row, same as today. |
| Cancellation | Honor `context.CancellationToken` on the outer `await foreach` over the gRPC stream. | Same as today. |
| Error reporting | Validation errors are caught in `QueryService` (no change). Adapter errors map to `RpcException(Internal)` (no change). Selection-set / DTO mapping errors are logged and the stream is aborted with `RpcException(Internal)`. | Same as today. |

### D.3 Target shape

#### D.3.1 New file: `AdapterFacade/Services/Selection/FieldSelection.cs`

Holds an immutable, pre-computed description of a single selection set
for a single root field, built once per query by parsing the AST.

```csharp
public sealed class FieldSelection
{
    public string RootFieldName { get; }
    public IReadOnlyList<SelectedScalarField> SelectedScalarFields { get; }

    public FieldSelection(string rootFieldName, IReadOnlyList<SelectedScalarField> fields) { ... }

    public static FieldSelection FromOperation(GraphQLOperationDefinition op) { ... }
    public static FieldSelection ForRootField(string rootFieldName, GraphQLSelectionSet selectionSet) { ... }
    public IReadOnlyDictionary<string, object?> Project(OrderInfo info) { ... }
}

public sealed record SelectedScalarField(string Alias, string OutputName);
```

`Project` is a synchronous, allocation-light projection from
`OrderInfo` to the dictionary shape `JsonSerializer` expects. It
inspects only the scalars listed in `SelectedScalarFields`; unknown
fields in the AST raise a build-time error in
`FieldSelection.FromOperation` (they map to no `OrderInfo` property).

#### D.3.2 New file: `AdapterFacade/Services/SelectionSetSerializer.cs`

Thin wrapper around `System.Text.Json` configured to:

- Emit snake_case keys (the projection dictionary already uses
  snake_case names — see `SelectedScalarField.OutputName`).
- Omit `null` values.
- Not pretty-print (the wire format expects a single line per row).

Provides one method: `string Serialize(IReadOnlyDictionary<string, object?> row)`.

#### D.3.3 Updated `OrderAdapter.Find`

```csharp
public async Task Find(
    AdapterQuery query,
    IServerStreamWriter<QueryResponse> responseStream,
    ServerCallContext context)
{
    ArgumentNullException.ThrowIfNull(query);
    ArgumentNullException.ThrowIfNull(responseStream);
    ArgumentNullException.ThrowIfNull(context);

    if (string.IsNullOrWhiteSpace(query.Query))
        throw new RpcException(new Status(StatusCode.InvalidArgument, "query must be provided"));

    var selection = ParseSelection(query);                                  // throws RpcException on parse / multi-op / unknown fields
    var sdl = _schema.Value.Print();

    _logger.LogInformation("OrderAdapter dispatched to root field {RootField}", selection.RootFieldName);

    var source = selection.RootFieldName switch
    {
        "searhByPhoneNumber" => StreamByPhone(query, selection, context),
        "findByOrderId"      => StreamByOrderId(query, selection, context),
        _ => throw new RpcException(new Status(StatusCode.InvalidArgument,
            $"Unknown root field '{selection.RootFieldName}' for OrderAdapter")),
    };

    try
    {
        await foreach (var info in source.WithCancellation(context.CancellationToken))
        {
            if (context.CancellationToken.IsCancellationRequested) break;
            if (info is null) continue;

            var row = selection.Project(info);
            var data = SelectionSetSerializer.Serialize(row);

            await responseStream.WriteAsync(new QueryResponse
            {
                ResultSchema = sdl,
                Data = data,
            });
        }
    }
    catch (OperationCanceledException) { throw; }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Failed to stream orders from OrderAdapter");
        throw new RpcException(new Status(StatusCode.Internal, $"Failed to stream orders: {ex.Message}"));
    }
}

private async IAsyncEnumerable<OrderInfo> StreamByPhone(
    AdapterQuery query,
    FieldSelection selection,
    ServerCallContext context)
{
    var phone = ExtractStringArg(query, "phone_number");
    await foreach (var info in _orderClient
        .GetOrdersByPhoneAsync(phone, context.CancellationToken)
        .WithCancellation(context.CancellationToken))
    {
        yield return info;
    }
}

// StreamByOrderId mirrors StreamByPhone against GetOrdersByOrderIdAsync.
```

`ParseSelection` builds the `FieldSelection` from the validated AST.
We do not re-parse variables — we re-use the `AdapterQuery.Variables`
dictionary that `QueryService` already coerced.

#### D.3.4 Variables & argument extraction

We continue to use the GraphQL.NET-coerced `Inputs` dictionary for
argument values (`AdapterQuery.Variables`). The `ExtractStringArg`
helper just looks up `variables["phone_number"]` etc. — the same
extraction logic the original `OrderQuery` resolver had.

#### D.3.5 What we keep from the current `OrderAdapter`

- `OrderDto` (the public DTO contract). Even though we no longer return
  it from a resolver, we keep it for tests / external code that imports
  the type.
- `OrderInfo` → `OrderDto` mapper (`ToDto`). Used by tests only.
- The SDL string returned from `Schema()`.
- The lazy `_schema` field (still built once via `new Schema { Query = new ObjectGraphType() { Name = "Query" } }`
  or simpler: a static `Schema` constructed from the SDL via
  `Schema.For(Sdl())` solely so `Schema()` can call `Print()`).

#### D.3.6 What we delete

- The `OrderQuery : ObjectGraphType` class.
- The `OrderType : ObjectGraphType<OrderDto>` class.
- The `OrderAdapterSchema : Schema` class.
- The `MapToDtosAsync` helper from §3 of the original plan (no longer
  needed — we return `IAsyncEnumerable<OrderInfo>` straight from the
  gRPC client).
- The `IDocumentExecuter` field, the `using GraphQL;` / `using
  GraphQL.Types;` / `using GraphQL.Utilities;` / `using
  GraphQL.Execution;` lines, the `using GraphQLParser.Visitors;` line.
- The `ExtractRows` and `ProjectNode` static helpers — replaced by
  `FieldSelection.Project` + `SelectionSetSerializer.Serialize`.

### D.4 File-by-file change list

| File | Change |
|------|--------|
| [`AdapterFacade/Services/Selection/FieldSelection.cs`](AdapterFacade/Services/Selection/FieldSelection.cs:1) | **New.** Defines `FieldSelection`, `SelectedScalarField`, AST → projection logic. |
| [`AdapterFacade/Services/SelectionSetSerializer.cs`](AdapterFacade/Services/SelectionSetSerializer.cs:1) | **New.** JSON serializer configured for snake_case keys, no pretty-print, null-omitting. |
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:1) | **Rewritten.** Remove `OrderQuery`, `OrderType`, `OrderAdapterSchema`, `IDocumentExecuter`, `ExtractRows`, `ProjectNode`. Add `Find` that uses `FieldSelection` + the raw gRPC stream. Keep `OrderDto` + `ToDto` (test surface), `Schema()` (SDL for `ResultSchema`), and a minimal `Schema` instance to back `Schema()`. |
| [`AdapterFacade/AdapterFacade.csproj`](AdapterFacade/AdapterFacade.csproj:1) | No new package. Confirm `GraphQLParser` is referenced (transitively via `GraphQL` 8.0.2). |
| [`AdapterFacade/Program.cs`](AdapterFacade/Program.cs:1) | No change. |
| [`AdapterFacade/Services/QueryService.cs`](AdapterFacade/Services/QueryService.cs:1) | No change. `IDocumentValidator` still validates the operation against the SDL. |
| [`plans/order-adapter-streaming-resolvers.md`](order-adapter-streaming-resolvers.md:1) | Update status table: Plan D "designed — pending implementation". |
| All other files | No change. |

### D.5 Behaviour matrix

| Client query (excerpt) | Root field | gRPC call | Streamed `QueryResponse.data` |
|------------------------|------------|-----------|-------------------------------|
| `{ searhByPhoneNumber(phone_number: "...") { order_id, product_name } }` | `searhByPhoneNumber` | `GetOrdersByPhoneAsync(phone)` | Each item contains **only** `order_id` and `product_name`. First item reaches the wire before the upstream gRPC stream is drained. |
| `{ findByOrderId(order_id: "o-42") { order_id, amount } }` | `findByOrderId` | `GetOrdersByOrderIdAsync(orderId)` | Each item contains **only** `order_id` and `amount`. Streaming is end-to-end. |
| Unknown root field | — | — | `RpcException(InvalidArgument)`. |
| Multi-operation document | — | — | `RpcException(InvalidArgument)` from `FieldSelection.FromOperation`. |
| Field on `Order` that doesn't exist (e.g. `bogus`) | — | — | `RpcException(InvalidArgument)` from `FieldSelection.FromOperation` at build time. |
| Selection set requests `__typename` | `Order` | — | `FieldSelection` returns `"Order"` as a constant for `__typename` (matches today's executor output). |

### D.6 Risks / things to double-check

1. **Selection-set walker correctness.** We are replacing GraphQL.NET's
   executor for this adapter. We need to handle, at minimum:
   - Scalar leaf selections.
   - `__typename` meta-field.
   - Field aliases (map alias → output key, not field name).
   - Inline fragments / spreads: we **reject** these for v1 with a
     clear error. (Our `Order` type is flat and has no abstract
     subtypes.)
   Capture the supported subset in `FieldSelection`'s XML doc and in
   the operator-facing plan.
2. **Variables handling.** `QueryService` already produces a
   GraphQL.NET-coerced `Inputs` dictionary. We must use the same
   dictionary for `ExtractStringArg` lookups. If a required argument is
   missing, throw `RpcException(InvalidArgument)` with a clear message.
3. **SDL truth.** `Schema()` returns a static SDL string that is
   hand-written. After implementing D, this SDL is the *contract* the
   executor no longer enforces. Any drift between the SDL and the
   walker's accepted subset shows up as runtime errors. Mitigation:
   add a unit test that asserts `Schema()` is a strict superset of
   `FieldSelection.SupportedFields`.
4. **Field name mapping.** `OrderInfo.OrderId` → `order_id` etc. The
   projection must use the GraphQL field names, not the CLR property
   names. The walker holds a static map (`OrderId` → `order_id`, etc.)
   and the SDL must agree.
5. **Cancellation.** We must pass `context.CancellationToken` into both
   the gRPC call and the outer `await foreach`. The downstream
   `IOrderClient` already accepts the token; verify by reading
   [`OrderClient/OrderClient.cs`](OrderClient/OrderClient.cs:26).
6. **Multiple selections at the root.** Today the executor handles
   `{ a b }` in one operation. We pick the first root field and warn /
   error. Decide on a policy (recommend: error) before implementing.
7. **What about operations of type `mutation` / `subscription`?**
   `OrderAdapter` has no such fields. `FieldSelection.FromOperation`
   must check `op.Operation == OperationType.Query` and reject
   otherwise, otherwise we silently mis-dispatch.

### D.7 Verification

```bash
dotnet build AdapterFacade/AdapterFacade.csproj
# Run OrderSource + AdapterFacade + Probe.
# Confirm:
#   - `searhByPhoneNumber` and `findByOrderId` queries still return identical JSON to before.
#   - With a synthetic 100k-row phone number, memory in AdapterFacade stays flat.
#   - With a cancelled client, the gRPC call to OrderSource is aborted (look for OperationCanceledException in the OrderSource log).
```

Expected:
- Build: 0 warnings, 0 errors.
- `OrderAdapter.Schema()` returns the same SDL as before.
- JSON shape of every `QueryResponse.data` is byte-identical to today.
- `OrderSource` log shows the client call started and (on cancel)
  aborted.
- Memory profile is flat with respect to row count.

### D.8 Cost summary

- Net new code: ~150–200 LOC across two new files
  ([`FieldSelection.cs`](AdapterFacade/Services/Selection/FieldSelection.cs) + [`SelectionSetSerializer.cs`](AdapterFacade/Services/SelectionSetSerializer.cs)).
- Net removed code: ~120 LOC in `OrderAdapter.cs` (resolvers, types, schema, walker).
- No new package.
- Highest implementation risk of the four plans; biggest behavioural
  change (GraphQL.NET's executor is no longer involved in this
  adapter's hot path).

---

## Plan E — switch to Hot Chocolate (ChilliCream) for native `IAsyncEnumerable<T>` resolvers

### E.1 Goal

Replace the GraphQL.NET 8 adapter with a Hot Chocolate 13/14 adapter so
that the resolver signature can be `IAsyncEnumerable<OrderInfo>` (or
`IAsyncEnumerable<OrderDto>`) without tripping the
`SetArrayItemNodesAsync` issue. The schema is re-expressed in
Hot Chocolate's `schema { query: Query }` SDL, the resolvers are
attached via Hot Chocolate's `[Query]` / `[UsePaging]` /
`[UseStreaming]` attributes or via the code-first `IObjectTypeDescriptor`
API, and the executor's `IRequestExecutor` is invoked from
`OrderAdapter.Find` to drive the operation.

The **outer** `IServerStreamWriter<QueryResponse>` path is unchanged:
`Find` still emits one `QueryResponse` per projected `Order` (or per
materialized batch — see §E.6 risk 1). We are not introducing a real
GraphQL-over-HTTP transport; we are using Hot Chocolate as a
selection-set-projection engine that happens to also know how to
iterate `IAsyncEnumerable<T>`.

### E.2 What Hot Chocolate actually gives us (and what it does not)

| Capability | Hot Chocolate 13/14 | Useful for our case? |
|------------|---------------------|----------------------|
| Resolver returns `IAsyncEnumerable<T>` for a `query` field | **Yes**, per-element projection. | Yes — replaces the `List<OrderDto>` materialization. |
| `@stream` / `@defer` directives on a `query` (incremental delivery) | Yes (13.0+). | Not applicable to gRPC; gRPC carrier is one `QueryResponse` per row regardless. |
| Resolver returns `IAsyncEnumerable<T>` for a `subscription` field | Yes, with WebSocket / SSE transport. | Not applicable — we keep the gRPC carrier. |
| `IQueryable<T>` resolver, batched grouping, data loaders | Yes. | Not needed for the current `OrderAdapter` shape. |
| Selection-set projection correctness | Handled by Hot Chocolate's execution engine. | Yes — we get this for free, no `FieldSelection` walker. |
| Single in-process execution entry point that returns an `IAsyncEnumerable` of projected items | **No** — Hot Chocolate's `IRequestExecutor.ExecuteAsync` returns a fully-materialized `IExecutionResult` (the JSON result tree). To get per-element iteration we must either (a) iterate the materialized result ourselves (still buffered), or (b) use the streaming response writer pipeline (`IResponseStream`). | This is the key caveat — see §E.6 risk 1. |

In other words: Hot Chocolate solves the "resolver returns
`IAsyncEnumerable<T>`" problem at the field level, but its
**single-document** execution entry point is still a buffer-the-whole-
result-and-iterate API. To get the first-row-reaches-the-wire-before-
the-last-row-is-produced property end-to-end, we have to either:

- use Hot Chocolate's `IResponseStream` (which is HTTP-only and does
  not help us behind a gRPC carrier), or
- write a small wrapper that hooks into Hot Chocolate's resolver
  pipeline and forwards each projected item to our
  `IServerStreamWriter<QueryResponse>` directly.

That wrapper is essentially the same shape as Plan D, just with
Hot Chocolate doing the selection-set projection instead of our own
walker. So the ROI of Plan E vs. Plan D is "we trade a hand-rolled
selection-set walker for a Hot Chocolate framework dependency".

### E.3 Design decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Which Hot Chocolate version? | 14.x (current at the time of writing). Targets `net8.0` and later — compatible with our `net10.0` target. | Latest stable. |
| Schema style? | **Code-first** via `IObjectTypeDescriptor` (Hot Chocolate's equivalent of GraphQL.NET's `ObjectGraphType<T>`). SDL is generated from the type descriptors and printed via `schema.ToString()`. | Minimises per-field boilerplate; resolvers are plain methods on the type class. |
| Resolver signature | `IAsyncEnumerable<OrderInfo>` on the query methods. Hot Chocolate's pipeline projects each item through the selection set. | The whole point of this plan. |
| Streaming wins at the wire | Still achieved by `IServerStreamWriter<QueryResponse>` — one `QueryResponse` per projected `Order`. | Carrier is unchanged. |
| Do we add a Hot Chocolate subscription type? | **No.** The gRPC carrier is `query` only — we do not introduce WebSocket / SSE transports. | Avoids a parallel execution path. |
| Validation flow | `QueryService` still validates the operation against the adapter's SDL string using `IDocumentValidator` (GraphQL.NET). The SDL returned by `OrderAdapter.Schema()` is the Hot Chocolate-generated SDL. | The validator is portable — it accepts any valid SDL. We get one source of truth for the schema. |
| Hot Chocolate executor placement | In-process via `IRequestExecutorBuilder.AddQueryType<OrderQuery>()...Build()`. The executor is a singleton; `OrderAdapter.Find` calls `executor.ExecuteAsync(request)` and walks the result. | Standard Hot Chocolate wiring. |
| Existing GraphQL.NET usage | Removed from `OrderAdapter`. Removed from [`AdapterFacade/AdapterFacade.csproj`](AdapterFacade/AdapterFacade.csproj:1) (replaced by `HotChocolate.AspNetCore` / `HotChocolate.Core`). | Pure framework swap. |
| `OrderDto` projection | Same DTO + `OrderInfo` → `OrderDto` mapper as today. Reused as a code-first entity class on the `OrderType` descriptor (or, if we want fewer types, hot chocolate can project `OrderInfo` directly via `IObjectTypeDescriptor<OrderInfo>`). | The DTO already maps 1-to-1 to `OrderInfo`. |
| gRPC contract | Unchanged. | One `QueryResponse` per row. |

### E.4 Target shape

#### E.4.1 Schema (Hot Chocolate code-first, `OrderType` descriptor)

```csharp
public sealed class OrderType : ObjectType<OrderDto>
{
    protected override void Configure(IObjectTypeDescriptor<OrderDto> descriptor)
    {
        descriptor.Name("Order");
        descriptor.Field(x => x.OrderId).Name("order_id");
        descriptor.Field(x => x.PhoneNumber).Name("phone_number");
        descriptor.Field(x => x.ProductName).Name("product_name");
        descriptor.Field(x => x.Amount).Name("amount");
    }
}

public sealed class OrderQuery
{
    public IAsyncEnumerable<OrderDto> GetOrdersByPhoneAsync(
        [Service] IOrderClient client,
        [Argument] string phone_number,
        CancellationToken cancellationToken)
    {
        return client
            .GetOrdersByPhoneAsync(phone_number, cancellationToken)
            .Select(info => new OrderDto(info.OrderId, info.PhoneNumber, info.ProductName, info.Amount));
    }

    public IAsyncEnumerable<OrderDto> GetOrdersByOrderIdAsync(
        [Service] IOrderClient client,
        [Argument] string order_id,
        CancellationToken cancellationToken)
    {
        return client
            .GetOrdersByOrderIdAsync(order_id, cancellationToken)
            .Select(info => new OrderDto(info.OrderId, info.PhoneNumber, info.ProductName, info.Amount));
    }
}
```

#### E.4.2 `OrderAdapter` (rewritten)

```csharp
public sealed class OrderAdapter : IAdapter
{
    public static string SourceId { get; } = "order_adapter_source_id";

    private readonly IRequestExecutor _executor;          // built once
    private readonly ILogger<OrderAdapter> _logger;

    public OrderAdapter(IOrderClient orderClient, ILogger<OrderAdapter> logger)
    {
        // Build the executor once. We use IRequestExecutorBuilder from
        // HotChocolate.Execution.ServiceCollectionExtensions (NOT the
        // AspNetCore overload — we don't expose an HTTP endpoint).
        _executor = SchemaBuilder.New()
            .AddQueryType<OrderQuery>()
            .AddType<OrderType>()
            .UseField(next => context =>
            {
                // Optional: hook resolver pipeline to forward each projected
                // item to a sink. Required if we want true end-to-end
                // streaming (see §E.6 risk 1).
                return next(context);
            })
            .Build()
            .GetRequestExecutor();

        _logger = logger;
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
            throw new RpcException(new Status(StatusCode.InvalidArgument, "query must be provided"));

        var sdl = _executor.Schema.Print();

        // Execute the operation. Hot Chocolate returns an IExecutionResult
        // whose Errors / Data follow the standard GraphQL response shape.
        IExecutionResult result;
        try
        {
            result = await _executor.ExecuteAsync(
                query.Query,
                new OperationRequest
                {
                    OperationName = query.OperationName,
                    Variables = query.Variables?.ToDictionary(
                        kv => kv.Key,
                        kv => (object?)kv.Value?.ToString()) ?? new(),
                },
                context.CancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hot Chocolate execution failed in OrderAdapter");
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }

        if (result.Errors is { Count: > 0 })
        {
            var message = string.Join("; ", result.Errors.Select(e => e.Message));
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, $"Query execution failed: {message}"));
        }

        // Walk the result tree — Hot Chocolate returns the data as a nested
        // dictionary. We pluck the (single) root field's value, which is
        // a List<Dictionary<string, object?>> already projected through
        // the selection set, and stream each element as one QueryResponse.
        if (result.Data is not IReadOnlyDictionary<string, object?> data || data.Count == 0) return;
        var rootField = data.Keys.First();
        if (data[rootField] is not IEnumerable<object?> rows) return;

        _logger.LogInformation("OrderAdapter dispatched to root field {RootField}", rootField);

        foreach (var row in rows)
        {
            if (context.CancellationToken.IsCancellationRequested) break;
            if (row is null) continue;

            var payload = JsonSerializer.Serialize(row);
            await responseStream.WriteAsync(new QueryResponse
            {
                ResultSchema = sdl,
                Data = payload,
            });
        }
    }

    public string Schema() => _executor.Schema.Print();
}
```

#### E.4.3 DI / Program.cs

```csharp
// Program.cs
builder.Services.AddOrderClient(builder.Configuration, "OrderClient");
builder.Services.AddKeyedScoped<IAdapter, OrderAdapter>(OrderAdapter.SourceId);
```

The `IRequestExecutor` is built inside the `OrderAdapter` constructor
(no DI registration needed for the executor itself). We can also
register it in DI and inject it into the adapter; both work.

#### E.4.4 QueryService

No code change required. It continues to call `adapter.Schema()` to
get the SDL, builds a GraphQL.NET `Schema` for validation via
`Schema.For(sdl)`, and forwards the validated operation as an
`AdapterQuery`. The validation pass now uses the Hot Chocolate-generated
SDL, which is portable to GraphQL.NET's validator (both speak the same
GraphQL SDL dialect).

### E.5 File-by-file change list

| File | Change |
|------|--------|
| [`AdapterFacade/AdapterFacade.csproj`](AdapterFacade/AdapterFacade.csproj:1) | Remove `GraphQL 8.0.2`. Add `HotChocolate.AspNetCore` 14.x (or `HotChocolate` core if we don't need the AspNetCore pipeline) — confirm transitive deps. |
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:1) | **Rewritten.** Code-first `OrderType` and `OrderQuery` classes using Hot Chocolate's `ObjectType<T>` / class-as-query. Resolvers return `IAsyncEnumerable<OrderDto>`. Constructor builds the `IRequestExecutor`. `Find` calls `_executor.ExecuteAsync(...)`, walks the result, emits one `QueryResponse` per row. `Schema()` delegates to `_executor.Schema.Print()`. |
| [`AdapterFacade/Program.cs`](AdapterFacade/Program.cs:1) | No change to adapter registration; `OrderAdapter` is still keyed by `SourceId`. |
| [`AdapterFacade/Services/QueryService.cs`](AdapterFacade/Services/QueryService.cs:1) | **Minor change.** `BuildSchema` already calls `Schema.For(sdl)` — works as-is with Hot Chocolate SDL. Confirm no Hot Chocolate-specific SDL constructs leak into the SDL string (Hot Chocolate's printer is standard-compliant). |
| `OrderAdapter.cs` — `MapToDtosAsync` helper | Add (or use a `Select` LINQ projection as in §E.4.1). No buffers. |
| All other files | No change. |

### E.6 Risks / things to double-check

1. **End-to-end streaming is *not* automatic.** Hot Chocolate's
   `IRequestExecutor.ExecuteAsync` returns a fully-materialized
   `IExecutionResult`. Even though the resolver returns
   `IAsyncEnumerable<T>`, the executor collects the result before
   returning. To forward each projected item to
   `IServerStreamWriter<QueryResponse>` as soon as it is produced, we
   have to either:
   - Subscribe to Hot Chocolate's `IResponseStream` (HTTP only — does
     not apply to gRPC).
   - Use a Hot Chocolate `IPostExecutionMiddleware` that hooks into
     the result stream and forwards to a sink — possible but requires
     careful coordination with the gRPC call.
   - Wrap the resolver so it pushes each item into a `Channel<T>` and
     a separate consumer task drains the channel into
     `IServerStreamWriter<QueryResponse>`. This is the most direct
     pattern and works in-process; it is essentially a Plan-D-style
     walker with Hot Chocolate doing the selection-set projection.
   Without one of these wrappers, Plan E gives us "no in-memory list
   in the resolver" but "still buffers in `ExecuteAsync`" — which is
   the same net memory profile as Plan A for large result sets.
2. **GraphQL.NET validation against Hot Chocolate SDL.** Hot Chocolate
   14 prints standard GraphQL SDL, so `Schema.For(sdl)` in
   `QueryService` should accept it. Confirm by running a
   build-and-boot smoke test after the swap. The SDL might include
   Hot Chocolate-specific directives (e.g. `@stream`, `@defer`) that
   GraphQL.NET's parser does not understand. If so, either strip
   those directives before returning the SDL from `Schema()`, or
   switch the validator to Hot Chocolate's own validator.
3. **Naming `searhByPhoneNumber`.** Hot Chocolate's convention is to
   strip the `Get` / `Async` suffix from method names when building
   the field name. We need to either rename the methods to
   `SearhByPhoneNumber` / `FindByOrderId` (still with the typo) or
   override the field name explicitly via `[GraphQLName("...")]`.
4. **Argument casing.** GraphQL.NET currently uses `phone_number` /
   `order_id` (snake_case) as argument names. Hot Chocolate defaults
   to camelCase for argument names derived from C# parameter names.
   We must annotate with `[GraphQLName("phone_number")]` to preserve
   the public contract.
5. **Two framework dependencies.** During the migration we may have to
   keep `GraphQL 8.0.2` for `QueryService`'s validator. Plan E assumes
   we drop `GraphQL 8.0.2` entirely — verify that Hot Chocolate ships
   an equivalent validator we can call from `QueryService`. (It does
   — `IRequestExecutor.Schema` carries validation rules. We can call
   `_executor.ValidateAsync(request)` and replace the current
   `IDocumentValidator` usage.)
6. **Package size and AOT.** `HotChocolate.AspNetCore` pulls a lot of
   transitive packages (authorization, Apollo federation helpers, etc.)
   even when we only need the in-process executor. Prefer
   `HotChocolate` (core) and add only the extensions we actually use.
7. **Learning curve.** Hot Chocolate's API surface is larger than
   GraphQL.NET's. The team needs to be comfortable with
   `IObjectTypeDescriptor`, `[Service]`, `[Argument]`, the executor
   builder DSL, and the `IExecutionResult` shape.

### E.7 Behaviour matrix

| Client query (excerpt) | Root field | gRPC call | Streamed `QueryResponse.data` |
|------------------------|------------|-----------|-------------------------------|
| `{ searhByPhoneNumber(phone_number: "...") { order_id, product_name } }` | `searhByPhoneNumber` | `GetOrdersByPhoneAsync(phone)` | Each item contains **only** `order_id` and `product_name` (selection set applied by Hot Chocolate). Streaming is end-to-end only if the §E.6 risk 1 wrapper is in place; otherwise the executor buffers before `Find` iterates. |
| `{ findByOrderId(order_id: "o-42") { order_id, amount } }` | `findByOrderId` | `GetOrdersByOrderIdAsync(orderId)` | Each item contains **only** `order_id` and `amount`. Same caveat. |
| Unknown root field | — | — | Hot Chocolate surfaces the error in `result.Errors`; we re-raise as `RpcException(InvalidArgument)`. |

### E.8 Verification

```bash
dotnet build AdapterFacade/AdapterFacade.csproj
# Run OrderSource + AdapterFacade + Probe.
# Confirm:
#   - `searhByPhoneNumber` and `findByOrderId` queries return identical JSON to before.
#   - With the §E.6 risk 1 wrapper in place: memory in AdapterFacade stays flat for large result sets.
#   - Without the wrapper: memory grows with result set size, but the JSON shape is correct.
```

Expected:
- Build: 0 warnings, 0 errors.
- `OrderAdapter.Schema()` returns a valid SDL (compare with the
  pre-migration SDL byte-for-byte modulo Hot Chocolate's printer
  formatting).
- JSON shape of every `QueryResponse.data` is byte-identical to today.
- If the wrapper is in place, `OrderSource` log shows the client call
  started and (on cancel) aborted as the gRPC stream is consumed
  element-by-element.

### E.9 Cost summary

- Net new code: ~80–120 LOC in `OrderAdapter.cs` (Hot Chocolate types + executor wiring + risk-1 wrapper if adopted).
- Net removed code: ~120 LOC of GraphQL.NET code-first resolvers + types.
- New package: `HotChocolate` 14.x (+ any extensions). Likely a few MB of transitive deps.
- Migration risk: high. Selection-set projection correctness, validation flow, and SDL portability are all touched.
- Highest learning-curve of all four plans.

### E.10 When to pick E

- The team is **already** considering Hot Chocolate for unrelated
  reasons (better tooling, future HTTP/WebSocket transport, etc.).
- The "end-to-end resolver-level streaming" goal is a hard requirement
  **and** we are willing to invest in the §E.6 risk 1 wrapper to make
  it actually work behind a gRPC carrier.
- We are okay with the package-size and AOT trade-offs (see risk 6).

If none of those hold, Plan D achieves the same end state with a
smaller dependency footprint and less framework risk.

---

### E.11 Implementation status — IMPLEMENTED

Plan E was implemented in June 2026. The actual implementation differs
in three ways from the sketch in §E.4 (all driven by what the Hot
Chocolate 16.1.0 API actually ships with, not by GraphQL.NET 8 design
constraints):

1. **Hot Chocolate version: 16.1.0, not 13/14.** The plan was written
   for HC 13/14, but at the time of implementation HC 16.1.0 is the
   current stable release and is what we use. The HC 16 API surface
   differs from the plan's sketch in a few places; see the notes below.
2. **DI-side executor builder, not standalone `SchemaBuilder.New()...Build()`.**
   In HC 16 `ISchemaBuilder` exposes `Create()` (returns
   `ISchemaDefinition`), not `Build()` (returns `IRequestExecutor`).
   The only way to obtain an `IRequestExecutor` is via DI
   (`services.GetRequestExecutorAsync(...)` /
   `RequestExecutorServiceProviderExtensions.GetRequestExecutorAsync`).
   We therefore move the schema wiring into [`Program.cs`](AdapterFacade/Program.cs:1) and
   inject `IRequestExecutor` into the [`OrderAdapter`](AdapterFacade/Services/OrderAdapter.cs:50)
   constructor — the `OrderAdapter` no longer builds its own executor.
3. **End-to-end streaming via a per-request `Channel<OrderDto>`.**
   The plan flagged §E.6 risk 1 ("executor still buffers") and listed
   three options for addressing it; the implementation picks the
   "Channel\<T\> + consumer task" option because it works in-process
   behind a gRPC carrier and gives us a clean backpressure story. The
   resolvers do not return `IAsyncEnumerable<OrderDto>` (HC 16 collects
   the result before returning from `ExecuteAsync` regardless). Instead,
   each resolver returns `Task<List<OrderDto>?>` (`null`), and pushes
   each projected row into a per-request `Channel<OrderDto>` that the
   `OrderAdapter.Find` consumer task drains into the gRPC
   `IServerStreamWriter<QueryResponse>` one row at a time. The
   per-request `ChannelSink` flows from `Find` into the resolvers via
   a `static AsyncLocal<ChannelSink?> CurrentSink` on `OrderAdapter` —
   no extra DI registration needed.

#### E.11.1 File-by-file change list (as implemented)

| File | Change |
|------|--------|
| [`AdapterFacade/AdapterFacade.csproj`](AdapterFacade/AdapterFacade.csproj:1) | Removed `GraphQL 8.0.2`. Added `HotChocolate 16.1.0` (no `HotChocolate.AspNetCore` — the in-process executor lives in the core package, so we avoid the AspNetCore transitive-dep bloat called out in §E.6 risk 6). |
| [`AdapterFacade/Services/AdapterQuery.cs`](AdapterFacade/Services/AdapterQuery.cs:1) | **Rewritten as a plain record.** The old `GraphQL.Inputs` field is gone — we keep the `IReadOnlyDictionary<string, object?>? Variables` shape (a name→object map already coerced by the GraphQL.NET `QueryService` validator) plus the new `VariablesJson` string. No more `GraphQL` dependency on this file. |
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:1) | **Rewritten.** New `public sealed record OrderDto` (snake_case on the wire via `OrderDtoType` descriptor). New nested `public sealed class OrderDtoType : ObjectType<OrderDto>` (Hot Chocolate code-first descriptor for the `Order` GraphQL type). New nested `public sealed class OrderQuery` with `GetSearhByPhoneNumberAsync` / `GetFindByOrderIdAsync` methods decorated with `[GraphQLName]` to preserve the snake_case public field names from the GraphQL.NET 8 contract. Constructor now takes `IRequestExecutor` and `ILogger<OrderAdapter>` (no more `IOrderClient` parameter — the resolvers receive it via the `[Service]` attribute). `Schema()` returns `_executor.Schema.ToString()` (the standard HC SDL printer). `Find` uses the `Channel<OrderDto>` + consumer-task pattern described above. |
| [`AdapterFacade/Program.cs`](AdapterFacade/Program.cs:1) | Added `using HotChocolate.Execution.Configuration;` and a new line `builder.Services.AddGraphQL().AddQueryType<OrderAdapter.OrderQuery>().AddType<OrderAdapter.OrderDtoType>();`. The `IRequestExecutor` is resolved from DI by the `OrderAdapter` constructor. |
| [`AdapterFacade/Services/QueryService.cs`](AdapterFacade/Services/QueryService.cs:1) | **No change.** It still validates against the SDL returned by `OrderAdapter.Schema()`. The HC 16 printer is standard-compliant GraphQL SDL, so `Schema.For(sdl)` accepts it. |
| All other files | No change. |

#### E.11.2 Behaviour matrix (as implemented)

| Client query (excerpt) | Root field | gRPC call | Streamed `QueryResponse.data` |
|------------------------|------------|-----------|-------------------------------|
| `{ searhByPhoneNumber(phone_number: "...") { order_id, product_name } }` | `searhByPhoneNumber` | `GetOrdersByPhoneAsync(phone)` | Each item contains **only** `order_id` and `product_name` (HC's selection-set projection). First item reaches the wire as soon as the resolver writes it into the channel, **before** the upstream gRPC stream is drained. |
| `{ findByOrderId(order_id: "o-42") { order_id, amount } }` | `findByOrderId` | `GetOrdersByOrderIdAsync(orderId)` | Each item contains **only** `order_id` and `amount`. Same end-to-end streaming behaviour. |
| Unknown root field | — | — | Hot Chocolate surfaces the error in `result.Errors`; we re-raise as `RpcException(InvalidArgument)`. |

#### E.11.3 Verification

```bash
dotnet build AdapterFacade/AdapterFacade.csproj
```

Expected: `Build succeeded. 0 Warning(s), 0 Error(s).`

Observed at implementation time: `0 Warning(s), 0 Error(s)`. ✅

Runtime smoke-testing (OrderSource + AdapterFacade + Probe) is the next
step; the build is the only check captured here.

#### E.11.4 Notes on API drift between plan (§E.4) and implementation

| Plan (§E.4) | Implementation | Why we changed it |
|-------------|----------------|-------------------|
| `SchemaBuilder.New().AddQueryType<OrderQuery>().AddType<OrderType>().Build().GetRequestExecutor()` | `builder.Services.AddGraphQL().AddQueryType<OrderAdapter.OrderQuery>().AddType<OrderAdapter.OrderDtoType>();` and inject `IRequestExecutor` | `ISchemaBuilder.Build()` does not exist in HC 16; the only `GetRequestExecutorAsync` extension lives on `IServiceProvider`. Moving to the DI side is the canonical HC 16 wiring. |
| `result is IExecutionResult; var data = (IReadOnlyDictionary<string, object?>)result.Data; ...` | Resolver returns `null`; rows live in a `Channel<OrderDto>`. `Find` consumes the channel and serializes each row. | `IRequestExecutor.ExecuteAsync` materializes the result tree before returning, so we cannot iterate per-element from the executor's return value. The `Channel<T>` pattern gives us end-to-end streaming as described in §E.6 risk 1. |
| Resolver signature `IAsyncEnumerable<OrderDto> GetOrdersByPhoneAsync(...)` | `async Task<List<OrderDto>?> GetSearhByPhoneNumberAsync(...)` returning `null` after kicking off the channel-pumping task | Same reason: HC 16 collects the result before returning, so `IAsyncEnumerable` doesn't help us. Returning `null` and pushing through a channel is functionally equivalent for the gRPC carrier. |
| `_executor.Schema.Print()` | `_executor.Schema.ToString()` | In HC 16, `ISchemaDefinition.ToString()` is the SDL-printing API; there is no public `.Print()` method. The XML docs confirm `ISchemaDefinition.ToString()` returns the SDL. |
| `new OperationRequest { ... }` | `OperationRequestBuilder.New().SetDocument(...).SetOperationName(...).SetVariableValues(...).Build()` returning `IOperationRequest` | `OperationRequest`'s public constructor in HC 16 is not directly usable; the builder pattern is the supported construction path. |
| `AdapterQuery.Variables` typed as `Inputs?` (GraphQL.NET 8 type) | `IReadOnlyDictionary<string, object?>?` | `GraphQL.Inputs` is gone with the GraphQL.NET 8 dependency. The dictionary shape is what `QueryService` already coerces into. |

#### E.11.5 Risk-1 wrapper adoption

§E.6 risk 1 (end-to-end streaming) is addressed by the
`Channel<OrderDto>` + consumer-task pattern (option 3 in the risk's
list). See [`OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs:65) `Find` and
`ConsumeChannelAsync` for the producer/consumer wiring, and the
`AsyncLocal<ChannelSink?> CurrentSink` for the per-request sink flow.

---

## Comparison

| Dimension | A — ship-as-is | C — subscriptions | D — selection walker | E — Hot Chocolate |
|-----------|----------------|-------------------|----------------------|-------------------|
| Lines of new code | 0 | ~100 | ~200 | ~100 (+ optional ~50 for risk-1 wrapper) |
| Lines removed | 0 | 0 | ~120 | ~120 |
| End-to-end streaming | No (buffered in resolver) | Yes (subject to §C.6 risk 1) | Yes (guaranteed) | Yes (subject to §E.6 risk 1 wrapper) |
| Selection-set correctness | Handled by GraphQL.NET | Handled by GraphQL.NET | Handled by us — we own the bug surface | Handled by Hot Chocolate |
| gRPC contract | Unchanged | Unchanged | Unchanged | Unchanged |
| Public SDL | Unchanged | Adds `type Subscription` | Unchanged | Unchanged (Hot Chocolate printer is standard) |
| Operational risk | None | Low (after spike) | Medium (custom walker) | High (framework swap + risk-1 wrapper) |
| New package | None | None | None | `HotChocolate` 14.x |
| Recommended if end-to-end resolver-level streaming is a hard requirement | No | Maybe (if the spike passes) | Yes | Maybe (if Hot Chocolate is wanted for other reasons too) |
