# Refactor plan: `OrderAdapter` — second query + in-process GraphQL execution

## 1. Goal recap

1. Add a second query field to `OrderAdapter`'s GraphQL schema. The new field
   has the **same parameters and return type shape** as the existing one but
   a **different name** (different lookup key).
2. Make `Find` dispatch on the incoming GraphQL query — it must look at the
   query's selected root field, resolve which underlying method to call, and
   apply the GraphQL selection set (only the fields the client asked for end
   up in the streamed JSON).
3. Use a real, library-grade GraphQL implementation **locally inside the
   adapter** — no HTTP endpoint exposed. Switch the schema construction to
   the **code-first** approach (GraphQL.NET types) so the schema is expressed
   in C# rather than as an SDL string.

Scope: primary file is `AdapterFacade/Services/OrderAdapter.cs`. Minimal
supporting changes are needed in `IAdapter` and in `QueryService` to pass the
GraphQL operation info through; `AbonentAdapter.cs` is currently fully
commented out and out of scope.

---

## 2. Design decisions (best-judgment)

| Question | Decision | Rationale |
|----------|----------|-----------|
| Name of the second query | `findByOrderId(order_id: String!): [Order!]!` | Natural counterpart to `searhByPhoneNumber`; same return type, different lookup key, matches the proto's `OrderInfo.order_id`. |
| Keep the typo `searhByPhoneNumber`? | **Yes, keep as-is.** | It's a public GraphQL field name — renaming it is a breaking change for any existing client/probe. New query gets a correctly spelled name. |
| Where does the "GraphQL server" live? | Inside the adapter, in-process, via GraphQL.NET's `IDocumentExecuter` over a code-first `Schema` instance. | The user explicitly asked for a library-grade local server. GraphQL.NET is already referenced (`GraphQL 8.0.2`) and its `IDocumentExecuter` is the standard in-process execution engine. No HTTP, no sockets. |
| Code-first vs schema-first SDL? | **Code-first** for the adapter schema. | The user explicitly allowed it. Code-first gives compile-time safety, typed resolvers, and a `Schema.Print()` we can still return from `Schema()` for backwards compatibility. |
| How is the selection set applied? | Resolver returns `IEnumerable<OrderDto>` (full DTOs); after `IDocumentExecuter.ExecuteAsync` runs, the resulting `ExecutionResult.Data` already contains only the requested fields as nested `Dictionary<string, object?>`. We serialize that subgraph as JSON and stream it. | GraphQL.NET already applies the selection set during execution — we don't need a hand-rolled selection-set walker. The cleanest path is: let the executor resolve and project, then re-serialize each top-level list element with only the fields the client selected. |
| `IAdapter.Find` signature | Add an `IResolveFieldContext`-like payload (or a small `AdapterQuery` DTO with `OperationName`, `SelectedFields`, and the coerced `Inputs`). | The adapter needs to (a) know which root field was selected and (b) see the arguments. Passing a typed `AdapterQuery` keeps the interface clean and avoids leaking GraphQL types across the interface boundary if we want. |
| `ResultSchema` value | Continue returning the printable SDL produced by `Schema.Print()` from the new code-first schema. | Preserves the proto contract — `QueryResponse.ResultSchema` keeps working for the gRPC stream consumer. |

---

## 3. Target shape of `OrderAdapter`

### 3.1 New `Schema()` output (printed from code-first `Schema`)

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
```

### 3.2 New code-first type definitions (private nested types in `OrderAdapter`)

- `OrderType : ObjectGraphType<OrderDto>`
  - Maps `order_id`, `phone_number`, `product_name`, `amount` 1-to-1 onto the proto's `OrderInfo`.
- `OrderQuery : ObjectGraphType`
  - `searhByPhoneNumber(phone_number: String!): [OrderType]` — resolver calls `_orderClient.GetOrdersByPhoneAsync(phone)`.
  - `findByOrderId(order_id: String!): [OrderType]` — resolver calls `_orderClient.GetOrdersByOrderIdAsync(orderId)` (new method on `IOrderClient` — see §5).
- `OrderAdapterSchema : Schema` — composed from `OrderQuery` and `OrderType`. `Schema.Print()` provides the SDL string returned from `Schema()`.

### 3.3 New `Find` flow

1. Validate args.
2. Build / cache the code-first `Schema` (a single per-adapter instance, created lazily in a field initializer — `Schema` construction is thread-safe after construction).
3. Use GraphQL.NET's `IDocumentExecuter` to execute the query **in-process**:
   ```csharp
   var result = await _executor.ExecuteAsync(opts =>
   {
       opts.Schema = _schema;
       opts.Query = request.Query;
       opts.Variables = inputs;          // already coerced in QueryService
       opts.OperationName = request.OperationName;
       opts.CancellationToken = context.CancellationToken;
   });
   ```
4. If `result.Errors` is non-empty, throw an `RpcException` with `StatusCode.InvalidArgument`.
5. Pull the root field name from `result.Operation.Operation` (or from the AST) to **log which query was dispatched** (this satisfies the "Find resolves which method to call" requirement — the actual dispatch already happened inside the resolvers, but the adapter still inspects the selected operation for logging/validation).
6. Walk `result.Data` to find the list returned by the selected root field, e.g. `(IEnumerable<object?>)result.Data["searhByPhoneNumber"]` or `result.Data["findByOrderId"]`. Each element is a `Dictionary<string, object?>` already containing only the **requested fields** — that's the selection-set application.
7. Stream each element serialized as JSON, with `ResultSchema = _schema.Print()`.
8. If the resolver stream throws (downstream gRPC error), wrap into `RpcException(Internal)` as today.

### 3.4 Selection-set application — why this works

GraphQL.NET resolvers receive `IResolveFieldContext` and, when the schema is
typed, they return full DTOs. The execution engine walks the selection set
and **only copies the requested fields into the output tree**. The resulting
`ExecutionResult.Data` is a nested `Dictionary<string, object?>` (with
`ExecutionNode`s for lists) where every key is a field the client actually
selected. Re-serializing that dictionary to JSON gives us the selection-set
output for free — no manual `FieldSelection` walker needed.

> Note: the existing `AdapterFacade/Services/Selection/FieldSelection.cs` and
> `SelectionSetSerializer.cs` files mentioned in the tab list are not present
> on disk. If/when they exist, the in-process executor output already does
> this job; we don't need to reimplement the walker.

### 3.5 Skeleton

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

    var sdl = _schema.Print(); // printable SDL exposed to gRPC clients

    ExecutionResult result;
    try
    {
        result = await _executor.ExecuteAsync(opts =>
        {
            opts.Schema = _schema;
            opts.Query = query.Query;
            opts.Variables = query.Variables?.ToInputs();
            opts.OperationName = query.OperationName;
            opts.CancellationToken = context.CancellationToken;
        });
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "GraphQL execution failed in OrderAdapter");
        throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
    }

    if (result.Errors is { Count: > 0 })
    {
        var message = string.Join("; ", result.Errors.Select(e => e.Message));
        throw new RpcException(new Status(StatusCode.InvalidArgument, $"Query execution failed: {message}"));
    }

    if (result.Data is null) return;

    // The selected root field is the only top-level key in result.Data for a
    // single-query document. The value is a list of dictionaries containing
    // ONLY the fields the client asked for (selection set already applied).
    var rootField = result.Data.Keys.First();
    if (result.Data[rootField] is not IEnumerable<object?> rows) return;

    _logger.LogInformation("OrderAdapter dispatched to root field {RootField}", rootField);

    foreach (var row in rows)
    {
        if (context.CancellationToken.IsCancellationRequested) break;
        if (row is null) continue;

        var data = JsonSerializer.Serialize(row);
        await responseStream.WriteAsync(new QueryResponse
        {
            ResultSchema = sdl,
            Data = data,
        });
    }
}
```

---

## 4. `IAdapter` changes (minimal)

Replace the current signature with one that carries the GraphQL operation
context, so the adapter can do its own dispatching instead of receiving
pre-extracted phone numbers.

```csharp
public record AdapterQuery(
    string Query,
    string? OperationName,
    string? VariablesJson,
    IReadOnlyDictionary<string, object?>? Variables);

public interface IAdapter
{
    Task Find(
        AdapterQuery query,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context);

    string Schema();
}
```

`AdapterQuery` is a small DTO in `AdapterFacade/Services/AdapterQuery.cs` —
not a GraphQL type, just a transport-agnostic carrier.

---

## 5. Supporting changes in `QueryService`

`QueryService` already validates the query and coerces variables. The new
flow it should drive:

1. Resolve the adapter (unchanged).
2. Build the **adapter's code-first** `Schema` once per request (or
   cache it on the adapter — the `Schema` is immutable after construction
   and safe to share; resolvers are invoked per-execution so instance state
   in resolvers is fine).
3. Validate the document against that schema (unchanged).
4. Build an `AdapterQuery` from `(request.Query, request.OperationName, request.Variables)` and pass it to `adapter.Find(...)`. **Stop pre-extracting phone numbers** — that's the adapter's job now.

`QueryService` no longer needs `ExtractPhoneNumbers` or any knowledge of
`phone_number` as a magic variable name.

---

## 6. New `IOrderClient` method

The second query looks up by `order_id`. Add to `OrderClient/IOrderClient.cs`
and implement in `OrderClient/OrderClient.cs`:

```csharp
IAsyncEnumerable<OrderInfo> GetOrdersByOrderIdAsync(
    string orderId,
    CancellationToken cancellationToken = default);
```

The implementation streams from a new gRPC method — either:
- a new RPC on `OrderService` (e.g. `GetOrdersByOrderId(OrderIdRequest) returns (stream OrderInfo)`), **or**
- a server-side filter inside the existing `GetOrdersByPhone` (out of scope, the gRPC contract would have to change anyway).

**Decision:** add a new RPC `GetOrdersByOrderId` to `Protos/order.proto`,
update `OrderSource` to implement it, and add the strongly-typed wrapper
to `IOrderClient`. This is the smallest correct change.

---

## 7. Package dependencies

`GraphQL 8.0.2` is already referenced. Confirm the following transitive
packages resolve (they ship with `GraphQL`):

- `GraphQL` — `IDocumentExecuter`, `Schema`, `ExecutionResult`, `Inputs`.
- `GraphQL.Types` — `ObjectGraphType<T>`, `Schema`.
- `GraphQL.SystemTextJson` — used by `Schema.Print()` and the default
  `IDocumentExecuter` writer; already pulled in by `GraphQL` 8.x.

If the executable needs `IGraphQLSerializer` for re-serializing the
selection-set output, use `SystemTextJson` (already available via
`GraphQL.SystemTextJson`). No new `PackageReference` should be required —
verify with a `dotnet restore` after the edits.

---

## 8. File-by-file change list

| File | Change |
|------|--------|
| `Protos/order.proto` | Add `rpc GetOrdersByOrderId (OrderIdRequest) returns (stream OrderInfo);` and the `OrderIdRequest { string order_id = 1; }` message. |
| `OrderSource/Services/OrderInfoService.cs` | Implement `GetOrdersByOrderId` (filter by `order_id` — same in-memory store pattern as the existing service). |
| `OrderClient/IOrderClient.cs` | Add `GetOrdersByOrderIdAsync(string orderId, CancellationToken)`. |
| `OrderClient/OrderClient.cs` | Implement `GetOrdersByOrderIdAsync` calling the new RPC. |
| `AdapterFacade/Services/AdapterQuery.cs` | **New.** `record AdapterQuery(string Query, string? OperationName, string? VariablesJson, IReadOnlyDictionary<string, object?>? Variables);` |
| `AdapterFacade/Services/IAdapter.cs` | Change `Find` signature to `(AdapterQuery query, IServerStreamWriter<QueryResponse>, ServerCallContext)`. |
| `AdapterFacade/Services/QueryService.cs` | Stop pre-extracting phone numbers. Build/pass `AdapterQuery`. Keep validation against the adapter's code-first schema. |
| `AdapterFacade/Services/OrderAdapter.cs` | **Primary refactor.** Replace SDL `Schema()` with a code-first `Schema` field. Define `OrderDto`, `OrderType`, `OrderQuery`, `OrderAdapterSchema`. Implement `Find` that runs the query in-process via `IDocumentExecuter` and streams the selection-set-projected rows as JSON. |
| `AdapterFacade/Program.cs` | No change. `OrderAdapter` is still registered by `SourceId`. |

`AdapterFacade/Services/AbonentAdapter.cs` remains commented-out (current
state). When/if it is revived, the same pattern applies but is **not** part
of this refactor.

---

## 9. Behavior matrix — both queries

| Client query (excerpt) | Root field dispatched | Resolver calls | Streamed JSON |
|------------------------|-----------------------|----------------|---------------|
| `{ searhByPhoneNumber(phone_number: "...") { order_id, product_name } }` | `searhByPhoneNumber` | `_orderClient.GetOrdersByPhoneAsync(phone)` | Each item contains **only** `order_id` and `product_name` (selection set applied by executor). |
| `{ findByOrderId(order_id: "o-42") { order_id, amount } }` | `findByOrderId` | `_orderClient.GetOrdersByOrderIdAsync(orderId)` | Each item contains **only** `order_id` and `amount`. |
| `{ searhByPhoneNumber(phone_number: "...") { order_id, phone_number product_name, amount } }` | `searhByPhoneNumber` | `_orderClient.GetOrdersByPhoneAsync(phone)` | Each item contains all four fields. |

The selection set is honored automatically by `IDocumentExecuter`. The
adapter does not have to enumerate requested fields by hand.

---

## 10. Risks / things to double-check during implementation

1. **GraphQL.NET list projection.** When a resolver returns `IEnumerable<T>`
   and the field type is `[Order!]!`, the executor projects each element
   through the `OrderType` definition. Confirm that scalar fields serialize
   with the snake_case names declared in the schema (`order_id`, etc.) — set
   the property name on each `Field(x => x.OrderId).Name("order_id")` (or
   use `[Name]` attribute) so the streamed JSON matches the contract.
2. **Schema caching.** Construct `_schema` once in a field initializer or in
   the constructor; do not build it per-call. The same applies to
   `IDocumentExecuter` — use `new DocumentExecuter()` once and reuse.
3. **Null root field.** Anonymous queries with multiple operations or no
   matching operation must be rejected. `ExecutionResult.Errors` will be
   populated; surface them as `InvalidArgument` `RpcException`.
4. **Streaming vs collecting.** The resolvers still stream from the gRPC
   client, but GraphQL.NET buffers the result of a field resolver before
   projecting children. That's fine for our payload sizes (an order list
   per phone), but means we lose true per-record streaming. This is an
   accepted consequence of "use a library GraphQL server".
5. **Backward compatibility.** `ResultSchema` keeps being the printable SDL
   so any consumer that reads it still sees the same shape. `Data` is
   now selection-set-projected — clients that previously expected every
   proto field will keep getting them as long as they ask for them in the
   query, which is the whole point of GraphQL.
6. **`AbonentAdapter` is out of scope.** It is fully commented out and
   unregistered. Do not touch.

---

## 11. Execution order (Code mode)

1. Update `Protos/order.proto` and regenerate the proto stubs.
2. Add `GetOrdersByOrderId` to `OrderSource/Services/OrderInfoService.cs`.
3. Add `GetOrdersByOrderIdAsync` to `OrderClient/IOrderClient.cs` and
   `OrderClient/OrderClient.cs`.
4. Create `AdapterFacade/Services/AdapterQuery.cs`.
5. Update `AdapterFacade/Services/IAdapter.cs` signature.
6. Refactor `AdapterFacade/Services/OrderAdapter.cs`:
   - introduce code-first `OrderType`, `OrderQuery`, `OrderAdapterSchema`,
     `OrderDto`;
   - lazy-initialize `_schema` and `_executor`;
   - rewrite `Find` to execute in-process and stream selection-set JSON;
   - keep `Schema()` returning `_schema.Print()`.
7. Update `AdapterFacade/Services/QueryService.cs` to stop pre-extracting
   phone numbers and to forward an `AdapterQuery`.
8. `dotnet build` the solution and fix any compile errors.

---

## 12. Open question for the user

The only thing this plan decides unilaterally is the **name of the second
query** (`findByOrderId`) and the **lookup key** (`order_id`). If you want
a different name (e.g. `ordersById`, `searchByOrderId`) or a different key
(e.g. `product_name`), say so before implementation and I'll adjust step 6
accordingly. Everything else is mechanically determined by the requirements
"same parameters and return value, different method name" and "in-process
GraphQL server with selection set applied".
