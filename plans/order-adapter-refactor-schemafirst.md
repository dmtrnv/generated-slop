# Refactor plan: `OrderAdapter` — second query + in-process GraphQL execution (schema-first variant)

> **Variant of** [`plans/order-adapter-refactor.md`](plans/order-adapter-refactor.md) with the **schema-first** approach.
> Same requirements, same scope, same target shape. Only the schema
> construction technique and the resolver-binding strategy change.
> The original plan is left untouched.

## 1. Goal recap

1. Add a second query field to `OrderAdapter`'s GraphQL schema. The new
   field has the **same parameters and return type shape** as the existing
   one but a **different name** (different lookup key).
2. Make `Find` dispatch on the incoming GraphQL query — it must look at
   the query's selected root field, resolve which underlying method to
   call, and apply the GraphQL selection set (only the fields the client
   asked for end up in the streamed JSON).
3. Use a real, library-grade GraphQL implementation **locally inside the
   adapter** — no HTTP endpoint exposed. Use the **schema-first**
   approach: the schema is expressed as SDL (a string the adapter
   already produces), and resolvers are attached programmatically on
   top of it. No `ObjectGraphType<T>` subclasses.

Scope: primary file is `AdapterFacade/Services/OrderAdapter.cs`. Minimal
supporting changes are needed in `IAdapter` and in `QueryService` to pass
the GraphQL operation info through. `AbonentAdapter.cs` is out of scope.

---

## 2. Design decisions (best-judgment, schema-first-specific)

| Question | Decision | Rationale |
|----------|----------|-----------|
| Name of the second query | `findByOrderId(order_id: String!): [Order!]!` | Same as the code-first plan — natural counterpart to `searhByPhoneNumber`. |
| Keep the typo `searhByPhoneNumber`? | **Yes, keep as-is.** | Public field name; renaming is a breaking change. New query gets a correctly spelled name. |
| How is the schema built? | **`Schema.For(sdl, configure)`** — the SDL is the single source of truth, the `configure` callback injects resolvers. | This is the canonical schema-first wiring in GraphQL.NET. The adapter's existing `Schema()` method already returns a valid SDL, so we keep it as the source string and extend it with the second root field. |
| How are resolvers attached? | In the `configure` callback, look up the `Query` type's auto-registered fields by **name** and assign `Resolver = new FuncFieldResolver<...>(...)`. | The SDL alone doesn't carry code, so we bind a C# delegate per root field by name. This keeps the schema definition declarative and the resolvers close to the field name. |
| Where do "DTO" types come from? | None — resolvers return `OrderInfo` (the generated proto type) directly. Field-level resolution still happens because the SDL's `type Order { ... }` is auto-resolved from the returned object's properties **via a registered `IAutoRegisteringHandler` or explicit `Field<T>` configuration**. | In schema-first mode GraphQL.NET does not auto-map arbitrary CLR types to GraphQL object types. We register each `Order` field explicitly inside `configure` against an `AutoRegisteringObjectGraphType<OrderInfo>` (one line per field), and let GraphQL.NET do the selection-set projection. |
| `IAdapter.Find` signature | Same as in the code-first plan: take an `AdapterQuery` DTO carrying `(Query, OperationName, VariablesJson, Variables)`. | Dispatch needs the original query string; the adapter does the actual execution. |
| `ResultSchema` value | Continue returning the SDL string from `Schema()`. The same string used to build the executable `Schema` instance. | The SDL **is** the schema, so `ResultSchema` literally is the source. No more `Print()` round-trip. |
| Schema caching | Build the `Schema` instance once in the constructor (lazy field). `Schema.For(...)` is expensive. | A single instance per adapter, reused across requests. Safe because `Schema` is immutable after construction. |

---

## 3. Target shape of `OrderAdapter` (schema-first)

### 3.1 `Schema()` — extended SDL string

The adapter already returns this SDL; we extend the `Query` type:

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

The string is built with a raw string literal (as today). The `Order` type
declares the projection shape that the selection set will be applied
against.

### 3.2 Building the executable `Schema` instance

```csharp
private readonly Schema _schema = BuildSchema();

private static Schema BuildSchema()
{
    var sdl = Sdl(); // returns the raw string above
    return Schema.For(sdl, configure =>
    {
        var query = configure.Types[typeof(Query)].As<QueryType>();

        query
            .Field<AutoRegisteringObjectGraphType<OrderInfo>>("searhByPhoneNumber")
            .Argument<NonNullGraphType<StringGraphType>>("phone_number")
            .Resolve(ctx =>
            {
                var phone = ctx.GetArgument<string>("phone_number");
                return ctx.RequestServices
                    .GetRequiredService<OrderAdapter>() // or capture via closure
                    .StreamOrdersByPhoneAsync(phone, ctx.CancellationToken);
            });

        query
            .Field<AutoRegisteringObjectGraphType<OrderInfo>>("findByOrderId")
            .Argument<NonNullGraphType<StringGraphType>>("order_id")
            .Resolve(ctx =>
            {
                var orderId = ctx.GetArgument<string>("order_id");
                return ctx.RequestServices
                    .GetRequiredService<OrderAdapter>()
                    .StreamOrdersByOrderIdAsync(orderId, ctx.CancellationToken);
            });
    });
}
```

A few notes that are easy to get wrong in schema-first mode:

- `AutoRegisteringObjectGraphType<OrderInfo>` maps the proto's
  `OrderInfo` CLR properties to GraphQL fields. The default behavior
  is to use the CLR property name (`OrderId`, `PhoneNumber`, ...). The
  SDL fields are `order_id`, `phone_number`, etc. Two options:
  1. **Add `[Name("order_id")]` etc. on `OrderInfo` properties** (proto
     generated types are `partial`, so we extend them with attributes
     in a separate `OrderInfoMap.cs`).
  2. **Or** use a small `OrderType : ObjectGraphType<OrderInfo>` that
     aliases CLR prop -> GraphQL field name (this is one tiny code-first
     helper class, not a full code-first schema — pragmatic and avoids
     editing the proto-generated partial class).
  **Decision: option 2.** Add a single helper class:
  ```csharp
  private sealed class OrderType : ObjectGraphType<OrderInfo>
  {
      public OrderType()
      {
          Name = "Order";
          Field(x => x.OrderId).Name("order_id");
          Field(x => x.PhoneNumber).Name("phone_number");
          Field(x => x.ProductName).Name("product_name");
          Field(x => x.Amount).Name("amount");
      }
  }
  ```
  This is **not** a code-first schema — it's a thin type alias used to
  teach the schema-first builder how to project a CLR object into the
  SDL's `Order` type. The schema structure (root fields, SDL string)
  is still schema-first.

- `Schema.For(sdl, configure)` is the right entry point. The
  `configure` callback receives an `IGraphQLBuilder` (schema-first
  configuration API) where we can pull out the auto-registered `Query`
  type and decorate it.

- Resolvers return `IAsyncEnumerable<OrderInfo>`. GraphQL.NET awaits
  the enumerator and projects each `OrderInfo` through the configured
  object type — selection set is honored automatically.

### 3.3 `Find` flow

1. Validate args (`AdapterQuery`, `responseStream`, `context`).
2. Run the query in-process via `IDocumentExecuter`:
   ```csharp
   var result = await _executor.ExecuteAsync(opts =>
   {
       opts.Schema = _schema;
       opts.Query = adapterQuery.Query;
       opts.Variables = adapterQuery.Variables?.ToInputs();
       opts.OperationName = adapterQuery.OperationName;
       opts.RequestServices = _serviceProvider; // so resolvers can resolve deps
       opts.CancellationToken = context.CancellationToken;
   });
   ```
3. If `result.Errors` is non-empty, throw `RpcException(InvalidArgument)`.
4. Determine which root field was selected (for logging and to satisfy
   the "Find resolves which method to call" requirement, even though
   the actual dispatch happened inside the resolvers):
   ```csharp
   var rootField = result.Operation?.SelectionSet.Selections
       .OfType<GraphQLParser.AST.GraphQLFieldSelection>()
       .FirstOrDefault()?.Name.StringValue;
   _logger.LogInformation("OrderAdapter dispatched to root field {RootField}", rootField);
   ```
5. `result.Data` is a `Dictionary<string, object?>` with one key (the
   root field name) whose value is a list of `Dictionary<string, object?>`
   — each element contains **only the fields the client selected**.
6. Stream each element as JSON, with `ResultSchema = Sdl()`.
7. If the resolver stream throws, wrap into `RpcException(Internal)`.

### 3.4 Skeleton

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

    var sdl = Sdl();

    ExecutionResult result;
    try
    {
        result = await _executor.ExecuteAsync(opts =>
        {
            opts.Schema = _schema;
            opts.Query = query.Query;
            opts.Variables = query.Variables?.ToInputs();
            opts.OperationName = query.OperationName;
            opts.RequestServices = _serviceProvider;
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

    if (result.Data is null || result.Data.Count == 0) return;

    // result.Data has exactly one key for a single-query document.
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

Identical to the code-first plan — `Find` takes an `AdapterQuery`:

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

Same as in the code-first plan — `QueryService` validates against the
adapter's `Schema` and forwards an `AdapterQuery`. It no longer
pre-extracts `phone_number` variables.

`QueryService` needs access to the **adapter's** `Schema` instance (not
the SDL string) to validate. Two options:
1. Add `Schema GetSchema()` to `IAdapter` (rename current `Schema()` to
   `SchemaSdl()` and add `GetSchema()`). Adapter owns the cached
   instance.
2. Re-parse SDL on every request via `Schema.For(adapter.Schema())`.

**Decision: option 1** — cached `Schema` is reused. `IAdapter` becomes:

```csharp
public interface IAdapter
{
    Task Find(AdapterQuery query, IServerStreamWriter<QueryResponse> stream, ServerCallContext context);
    string Schema();         // SDL (for QueryResponse.ResultSchema and for back-compat)
    global::GraphQL.Types.Schema GetSchema(); // cached executable schema
}
```

---

## 6. New `IOrderClient` method

Identical to the code-first plan. Add `GetOrdersByOrderIdAsync` to
`IOrderClient` and implement via a new `GetOrdersByOrderId` RPC on
`Protos/order.proto`.

---

## 7. Package dependencies

`GraphQL 8.0.2` is already referenced. Schema-first mode needs nothing
extra. Confirm these names resolve:
- `GraphQL.Schema` (the `Schema.For(sdl, configure)` entry point)
- `GraphQL.Types` (`ObjectGraphType<T>`, `AutoRegisteringObjectGraphType<T>`, `StringGraphType`, `NonNullGraphType`, `IGraphQLBuilder`)
- `GraphQL.Resolvers` (`FuncFieldResolver<T>` — only needed if we don't use the lambda-overload of `Resolve`)

If `AutoRegisteringObjectGraphType<T>` proves awkward for the
`Order` -> `OrderInfo` mapping, we use the tiny `OrderType` helper
described in §3.2.

---

## 8. File-by-file change list (schema-first)

| File | Change |
|------|--------|
| `Protos/order.proto` | Add `rpc GetOrdersByOrderId (OrderIdRequest) returns (stream OrderInfo);` and `OrderIdRequest { string order_id = 1; }`. |
| `OrderSource/Services/OrderInfoService.cs` | Implement `GetOrdersByOrderId` (filter by `order_id` — same in-memory store pattern as the existing service). |
| `OrderClient/IOrderClient.cs` | Add `GetOrdersByOrderIdAsync(string orderId, CancellationToken)`. |
| `OrderClient/OrderClient.cs` | Implement `GetOrdersByOrderIdAsync` calling the new RPC. |
| `AdapterFacade/Services/AdapterQuery.cs` | **New.** `record AdapterQuery(string Query, string? OperationName, string? VariablesJson, IReadOnlyDictionary<string, object?>? Variables);` |
| `AdapterFacade/Services/IAdapter.cs` | Change `Find` to take `AdapterQuery`. Add `Schema GetSchema()`. Keep `string Schema()` for the SDL payload. |
| `AdapterFacade/Services/QueryService.cs` | Use `adapter.GetSchema()` for validation. Build/pass `AdapterQuery`. Stop pre-extracting phone numbers. |
| `AdapterFacade/Services/OrderAdapter.cs` | **Primary refactor.** Keep SDL as the single source string. Add a private `OrderType` helper (or use `AutoRegisteringObjectGraphType<OrderInfo>`) to teach the schema-first builder how to project `OrderInfo` into the `Order` SDL type. Build a cached `Schema` via `Schema.For(sdl, configure)` with two resolvers bound to the existing `searhByPhoneNumber` and new `findByOrderId` fields. Rewrite `Find` to run the query in-process and stream selection-set-projected JSON. |
| `AdapterFacade/Program.cs` | No change. `OrderAdapter` is still registered by `SourceId`. |

`AdapterFacade/Services/AbonentAdapter.cs` remains commented-out (current
state) and is **not** part of this refactor.

---

## 9. Behavior matrix — both queries (unchanged from code-first plan)

| Client query (excerpt) | Root field dispatched | Resolver calls | Streamed JSON |
|------------------------|-----------------------|----------------|---------------|
| `{ searhByPhoneNumber(phone_number: "...") { order_id, product_name } }` | `searhByPhoneNumber` | `StreamOrdersByPhoneAsync(phone)` | Each item contains **only** `order_id` and `product_name` (selection set applied by executor through `OrderType`). |
| `{ findByOrderId(order_id: "o-42") { order_id, amount } }` | `findByOrderId` | `StreamOrdersByOrderIdAsync(orderId)` | Each item contains **only** `order_id` and `amount`. |

---

## 10. Risks / things to double-check (schema-first-specific)

1. **`Schema.For(sdl, configure)` API surface in GraphQL.NET 8.x.** The
   `configure` callback receives an `IGraphQLBuilder` (or
   `SchemaConfiguration`). The exact method names for "decorate an
   already-registered field" differ slightly across versions. Before
   writing the code, verify against the installed `GraphQL 8.0.2`:
   - `configure.Types[typeof(Query)].As<QueryType>()` may be
     `configure.Types.Query` in 8.x.
   - The `Field<TGraph>("name")` chained method names need to match
     the actual API.
   Fallback: drop down to `configure.Types.For("Query").Field<...>`
   and `configure.Types.For("Order").Field<...>` for string-based
   type lookup that does not depend on the exact `Query` CLR type.

2. **Two `Order` definitions.** Because we use a tiny code-first
   `OrderType` to map `OrderInfo` -> `Order` SDL fields, the SDL's
   `type Order { ... }` declaration and the `OrderType` CLR type must
   agree on every field. Mismatches surface as schema-build errors
   ("field X is not defined on type Y" or similar). Build the schema
   in the constructor and let it fail fast on startup — invalid SDL
   becomes a deployment bug, not a runtime bug.

3. **Resolver dependency injection.** Schema-first resolvers do not
   have constructor injection like `ObjectGraphType<T>` resolvers do.
   Pass `opts.RequestServices = _serviceProvider` when executing and
   have the resolver delegate pull `OrderAdapter` (or
   `IOrderClient`) from `ctx.RequestServices`. Alternatively, capture
   them via closure when building the schema. Closure capture is
   simpler and works because the adapter owns the resolvers.

4. **`IAsyncEnumerable` in schema-first resolvers.** GraphQL.NET 8
   awaits `IAsyncEnumerable<T>` returned from a field resolver. The
   downstream gRPC client streams `OrderInfo`; awaiting the
   enumerator gives the executor a fully-materialized list of
   `OrderInfo`, after which the `OrderType` projection applies the
   selection set and builds the response tree. This is fine for our
   payload sizes (orders per phone/order-id).

5. **Result data is a `Dictionary<string, object?>` with snake_case
   keys.** The `OrderType` helper sets `Name("order_id")` etc., so the
   projected dictionaries use snake_case keys. JSON serialization of
   those dictionaries preserves the names — no naming surprises.

6. **Backward compatibility of `QueryResponse.ResultSchema`.** We
   return the same SDL string the adapter always returned (now with
   the extra `findByOrderId` field). Clients that introspect the SDL
   see a strict superset. Clients that only ever ask for
   `searhByPhoneNumber` are unaffected.

7. **Error path from resolvers.** GraphQL.NET catches exceptions
   thrown inside resolvers and adds them to `result.Errors`. We
   surface that as an `RpcException(InvalidArgument)` — that loses
   the stack trace from the gRPC client's perspective but is
   consistent with how `QueryService` already handles validation
   errors. If we want a different code, branch on whether the error
   originated from `_orderClient` (use `Internal`) vs. from
   argument parsing (use `InvalidArgument`).

---

## 11. Execution order (Code mode)

1. Update `Protos/order.proto` and regenerate the proto stubs.
2. Add `GetOrdersByOrderId` to `OrderSource/Services/OrderInfoService.cs`.
3. Add `GetOrdersByOrderIdAsync` to `OrderClient/IOrderClient.cs` and
   `OrderClient/OrderClient.cs`.
4. Create `AdapterFacade/Services/AdapterQuery.cs`.
5. Update `AdapterFacade/Services/IAdapter.cs`: change `Find`
   signature, add `GetSchema()`.
6. Refactor `AdapterFacade/Services/OrderAdapter.cs`:
   - extend the `Sdl()` (or `Schema()`) string with the second
     query;
   - add private `OrderType : ObjectGraphType<OrderInfo>` helper for
     snake_case field mapping;
   - add cached `_schema` field built via
     `Schema.For(Sdl(), configure)` with two resolvers bound to
     `_orderClient` via closure capture;
   - add cached `_executor = new DocumentExecuter()`;
   - rewrite `Find` to execute in-process and stream
     selection-set-projected JSON.
7. Update `AdapterFacade/Services/QueryService.cs` to use
   `adapter.GetSchema()` for validation and to forward an
   `AdapterQuery`.
8. `dotnet build` the solution and fix any compile errors.

---

## 12. Open question for the user

Same as the code-first plan: I picked `findByOrderId` / `order_id` as
the second query. If you want a different name (e.g. `ordersById`,
`searchByOrderId`) or a different lookup key (e.g. `product_name`),
say so before implementation and I'll adjust step 6 accordingly.
Everything else is mechanically determined by "schema-first instead
of code-first, otherwise same requirements".
