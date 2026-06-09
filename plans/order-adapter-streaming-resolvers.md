# Plan: stream `OrderAdapter` resolvers (no in-memory list)

## 1. Status: ✅ Plan C (GraphQL subscriptions) implemented

The original Plan A attempt (return `IAsyncEnumerable<OrderDto>` from the resolvers)
**does not work** against `GraphQL 8.0.2`'s `SerialExecutionStrategy` for the list-field
path (see the historical runtime evidence captured below). To get true end-to-end
streaming we have implemented **Plan C** from the options list in §4:

- A new `Subscription` root type was added to the code-first schema in
  [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs)
  with two fields — `searhByPhoneNumber(phone_number: String!): Order` and
  `findByOrderId(order_id: String!): Order` — each wired with both a `FuncFieldResolver`
  (trivial `default!` return; the `SubscriptionExecutionStrategy` does not call it) and
  a `SourceStreamResolver` whose delegate returns an `IObservable<OrderDto>` produced
  by the new `MapToDtosAsync` helper (one `OrderDto` per upstream `OrderInfo`) bridged
  through the in-repo `AsyncEnumerableObservable` adapter (avoids the `System.Reactive`
  dependency).
- The DI container in [`AdapterFacade/Program.cs`](AdapterFacade/Program.cs) now
  registers a singleton `SubscriptionExecutionStrategy` and a singleton
  `IDocumentExecuter` constructed with a `DefaultExecutionStrategySelector` whose
  `ExecutionStrategyRegistration`s map `OperationType.Query` / `OperationType.Mutation`
  → `SerialExecutionStrategy` (unchanged) and `OperationType.Subscription` →
  `SubscriptionExecutionStrategy`. The `DocumentExecuter` ctor takes
  `(IDocumentBuilder, IDocumentValidator, IExecutionStrategySelector, IEnumerable<IConfigureExecution>)`
  — the only public ctor that accepts a strategy selector in GraphQL 8.0.2.
- The existing `Query` root still returns `[Order]` lists materialized from
  `List<OrderDto>` (Plan A behaviour, unchanged) so the gRPC carrier contract is
  preserved. Subscriptions are additive; the SDL printed by `OrderAdapter.Schema()`
  now contains both root types.
- `OrderAdapter` now takes the `IDocumentExecuter` from DI; the secondary
  parameterless chained ctor was removed. The constructor signature is the one
  `Microsoft.Extensions.DependencyInjection` resolves.
- The gRPC carrier (`IServerStreamWriter<QueryResponse>`) is unchanged. For
  subscription operations the adapter walks the single `RootExecutionNode` that
  `SubscriptionExecutionStrategy` produces (per Plan C §C.6 risk 1, the strategy may
  buffer the final `ExecutionResult` and emit one row per `OrderInfo` event).
- Build: `dotnet build WebApplication2.sln` → **Build succeeded. 0 Warning(s), 0 Error(s).**
- Smoke test: a throwaway probe in `AdapterFacade/bin/apiprobe/` (since deleted)
  constructed `OrderAdapter` with stub `IOrderClient` / `IDocumentExecuter`
  implementations, called `adapter.Schema()`, and confirmed the printed SDL contains
  `type Subscription { searhByPhoneNumber(phone_number: String!): Order, findByOrderId(order_id: String!): Order }`.

Historical runtime evidence (the original Plan A failure, preserved for context):

```
GraphQL.Execution.UnhandledError: Error trying to resolve field 'searhByPhoneNumber'.
 ---> System.InvalidOperationException: Expected an IEnumerable list though did not find one.
      Found: <MapToDtosAsync>d__19
   at GraphQL.Execution.ExecutionStrategy.SetArrayItemNodesAsync(ExecutionContext context,
        ArrayExecutionNode parent) in /_/src/GraphQL/Execution/ExecutionStrategy.cs:line 425
   at GraphQL.Execution.ExecutionStrategy.CompleteNodeAsync(ExecutionContext context,
        ExecutionNode node) in /_/src/GraphQL/Execution/ExecutionStrategy.cs:line 587
```

`ExecutionStrategy.SetArrayItemNodesAsync` checks the field's `Result` and **requires a
synchronous `IEnumerable`** on the list-field path. The Plan A code change that
introduced the error was **reverted** for the `Query` root; the streaming goal is now
met on the `Subscription` root instead, where `SubscriptionExecutionStrategy` iterates
the `IAsyncEnumerable<OrderDto>` natively.

## 2. Original goal (still valid)

The two GraphQL root-field resolvers (`searhByPhoneNumber`, `findByOrderId`) in
[`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs)
consume an `IAsyncEnumerable<OrderInfo>` from the gRPC `IOrderClient` but accumulate
every row into a `List<OrderDto>` before returning. For an order source with thousands
of rows per lookup this defeats the point of the streamed gRPC contract, even though
the adapter's own `Find` loop writes one `QueryResponse` at a time.

## 3. What I tried (and why it failed)

I rewrote both resolvers to return `IAsyncEnumerable<OrderDto>` directly via a new
[`MapToDtosAsync`](AdapterFacade/Services/OrderAdapter.cs) helper:

```csharp
return MapToDtosAsync(
    orderClient.GetOrdersByPhoneAsync(phone, context.CancellationToken),
    context.CancellationToken);
```

The helper yielded one DTO per `OrderInfo`. The build was green. The runtime failed
with the error above — `SetArrayItemNodesAsync` sees the `IAsyncEnumerable<OrderDto>`
state machine, can't cast it to `IEnumerable`, and throws.

## 4. Correct options going forward (in increasing order of effort)

### Option A — accept the in-memory list at the resolver layer (current state)

**What changes:** nothing. The resolvers continue to return `List<OrderDto>`; the
adapter-level streaming (one `QueryResponse` per row via `IServerStreamWriter`)
remains the source of all streaming wins.

**Trade-off:** for very large order sets (e.g. > 100k rows per phone), the resolver
allocates a `List<OrderDto>` proportional to the result size before the executor
begins iterating. This is acceptable for the current expected order volumes (the
proto / contracts are designed around per-phone lookups, not per-customer bulk
exports).

**Status:** active (default behaviour for the `Query` root; the in-memory
`List<OrderDto>` stays there for the list-field path that
`SerialExecutionStrategy` enforces).

### Option B — pre-stream into a shared collection that's still `IEnumerable`

If we need a stable `IEnumerable<OrderDto>` for the executor, we can either:

1. Return `OrderDto[]` instead of `List<OrderDto>` — same memory cost, slightly
   cleaner type. Zero benefit, **not worth doing**.
2. Hand back an `IEnumerable<OrderDto>` backed by a custom `IEnumerator<T>` that
   pulls from the upstream `IAsyncEnumerable<OrderInfo>` via the executor's
   `SynchronizationContext` / blocking bridge. This blocks the executor thread on
   the gRPC stream and re-introduces per-row latency. **Worse than option A** for
   the common case.

### Option C — use a GraphQL subscription instead of a query

GraphQL subscriptions are designed for streaming, and GraphQL.NET 8 supports
`IAsyncEnumerable<T>` returns from subscription resolvers. To get true end-to-end
streaming we would:

1. Promote `searhByPhoneNumber` / `findByOrderId` to GraphQL subscription fields
   (or add a new pair of subscription fields next to them so the gRPC contract
   remains backward-compatible).
2. Add a `Subscription` root type with two `AddField<EventStreamType, ...>` entries
   whose resolvers return `IAsyncEnumerable<OrderDto>` produced by the
   [`MapToDtosAsync`](AdapterFacade/Services/OrderAdapter.cs) helper (the same
   helper, re-purposed for subscriptions).
3. The executor's `SubscriptionExecutionStrategy` already iterates the
   `IAsyncEnumerable` per source event and projects each item through the selection
   set, so the in-memory `List<OrderDto>` goes away naturally.
4. Switch the gRPC `QueryResponse` semantics: instead of "one gRPC message per
   row, all driven by a single query", it would be "one gRPC message per
   subscription event". Wire-compatible if we just stream the events on the same
   channel.

**Trade-off:** subscriptions change the executor strategy and the conceptual
model ("one query, one list" vs. "one subscription, N events"). They also require
the executor to be wired with `SubscriptionExecutionStrategy` in
`Program.cs` / DI. This is the right long-term path if true streaming of the
resolver is a hard requirement.

**Status:** ✅ **Implemented** (see §1, §6, §7 for the diff and the smoke test).

### Option D — drop GraphQL.NET for this adapter and parse the query ourselves

The other end of the spectrum: bypass `IDocumentExecuter` entirely, walk the
selection set with the existing
[`AdapterFacade/Services/Selection/FieldSelection.cs`](AdapterFacade/Services/Selection/FieldSelection.cs)
walker, and write one `QueryResponse` per `OrderInfo` directly out of the
`IAsyncEnumerable<OrderInfo>` from the gRPC client. This is essentially what
`AbonentAdapter` (currently fully commented out) used to do. The selection-set
walker is the "FieldSelection" file referenced in the project tree (not present
on disk yet — see `plans/order-adapter-refactor-schemafirst.md`).

**Trade-off:** abandons the code-first schema for this adapter, but it does
fully achieve the "no in-memory list, stream from source to wire" goal without
touching GraphQL.NET internals.

## 5. Recommendation

If the goal is **eliminating the in-memory `List<OrderDto>` for large order
sets**:

1. ✅ **Short term (Option A, current code):** ship as-is. The streaming wins on
   the wire (`IServerStreamWriter<QueryResponse>`) are real, and per-phone order
   sets are bounded in the current data model. *(Active: the `Query` root still
   returns `List<OrderDto>`.)*
2. ✅ **Medium term (Option C):** add a subscription path. This is the only
   GraphQL.NET 8 native way to use `IAsyncEnumerable<T>` resolvers. It is the
   right architectural fit if the order source ever starts emitting
   firehose-style result sets. *(Implemented: see §6 / §7.)*
3. **Long term (Option D):** if subscriptions prove cumbersome (they require a
   different transport semantics on the gRPC side), drop the in-process
   `IDocumentExecuter` for this adapter and use the selection-set walker over
   the raw gRPC stream. This is the most invasive change but gives full control
   over memory and latency.

## 6. File-by-file change list (current)

| File | Change |
|---|---|
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs) | **Plan C implementation.** Added `using GraphQL.Resolvers;` for `FuncFieldResolver<>` / `SourceStreamResolver<>`. Added `OrderSubscription : ObjectGraphType` with two subscription fields (`searhByPhoneNumber(phone_number: String!): Order`, `findByOrderId(order_id: String!): Order`) — each wired with a trivial `FuncFieldResolver<OrderDto?>` (returns `default!`, never called by `SubscriptionExecutionStrategy`) and a `SourceStreamResolver<OrderDto>` whose delegate returns an `IObservable<OrderDto>` produced by the new `MapToDtosAsync` helper, bridged through the in-repo `file static class AsyncEnumerableObservable` (no `System.Reactive` dependency). The `OrderAdapterSchema` ctor now accepts a `Subscription` and assigns it. The `Query` root still materializes `List<OrderDto>` (Plan A behaviour, unchanged). The two `OrderQuery` resolvers and the `[GraphQL]`-decorated DTOs are unchanged. |
| [`AdapterFacade/Program.cs`](AdapterFacade/Program.cs) | **Plan C implementation.** Added `using GraphQL.Validation;` (for `DocumentValidator`) and `using GraphQLParser.AST;` (for `OperationType`). Added `using GraphQL.DI;` for `IConfigureExecution`. Registered `SubscriptionExecutionStrategy` as a singleton and wired a singleton `IDocumentExecuter` with a `DefaultExecutionStrategySelector` whose `ExecutionStrategyRegistration[]` maps `Query` / `Mutation` → `SerialExecutionStrategy` and `Subscription` → the new `SubscriptionExecutionStrategy`. The `DocumentExecuter` is constructed via the 4-arg public ctor `(IDocumentBuilder, IDocumentValidator, IExecutionStrategySelector, IEnumerable<IConfigureExecution>)` with `GraphQLDocumentBuilder` + `DocumentValidator` defaults. |
| All other files | No change. |

## 7. Verification

- `dotnet build WebApplication2.sln` → **Build succeeded. 0 Warning(s), 0 Error(s).**
- Throwaway probe (`AdapterFacade/bin/apiprobe/`, since deleted) constructed
  `OrderAdapter` with stub `IOrderClient` / `IDocumentExecuter` implementations,
  called `adapter.Schema().Print()`, and asserted that the SDL contains
  `type Subscription` with both `searhByPhoneNumber(phone_number: String!): Order`
  and `findByOrderId(order_id: String!): Order`. The probe printed the expected
  `type Subscription { ... }` block.
- The original Plan A runtime error (`SetArrayItemNodesAsync` → "Expected an
  IEnumerable list though did not find one") is no longer reachable on the
  `Subscription` root, because `SubscriptionExecutionStrategy` projects each
  `IAsyncEnumerable<OrderDto>` item through the selection set without forcing
  it into a synchronous `IEnumerable`.

## 8. Resolved

Plan C is implemented (see §1, §6, §7). The `Subscription` root now exposes
`IAsyncEnumerable<OrderDto>` resolvers via `SourceStreamResolver` and
`MapToDtosAsync`, with the executor wired to `SubscriptionExecutionStrategy`
through `DefaultExecutionStrategySelector`. The historical A / C / D question
is closed: **C chosen and shipped**; the gRPC carrier contract is preserved;
the `Query` root continues to return `List<OrderDto>` (Plan A) so the
serialization layer is unchanged. Option D (selection-set walker over the raw
gRPC stream) remains the documented long-term escape hatch if subscription
transport ever proves insufficient.
