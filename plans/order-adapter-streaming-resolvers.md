# Plan: stream `OrderAdapter` resolvers (no in-memory list)

## 1. Status: deferred — runtime blocker discovered

The original plan (return `IAsyncEnumerable<OrderDto>` from the resolvers) **does not work**
against `GraphQL 8.0.2`. Runtime evidence:

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
synchronous `IEnumerable`**. `IAsyncEnumerable<T>` resolvers are not supported on the
list-field path in GraphQL.NET 8.0.2.

The code change that introduced the error has been **reverted**: the two `OrderQuery`
resolvers in [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs)
are back to materializing into `List<OrderDto>` and returning it, with a code comment
pointing at this plan. The file builds cleanly (0 warnings, 0 errors).

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

**Status:** active.

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

1. **Short term (Option A, current code):** ship as-is. The streaming wins on
   the wire (`IServerStreamWriter<QueryResponse>`) are real, and per-phone order
   sets are bounded in the current data model.
2. **Medium term (Option C):** add a subscription path. This is the only
   GraphQL.NET 8 native way to use `IAsyncEnumerable<T>` resolvers. It is the
   right architectural fit if the order source ever starts emitting
   firehose-style result sets.
3. **Long term (Option D):** if subscriptions prove cumbersome (they require a
   different transport semantics on the gRPC side), drop the in-process
   `IDocumentExecuter` for this adapter and use the selection-set walker over
   the raw gRPC stream. This is the most invasive change but gives full control
   over memory and latency.

## 6. File-by-file change list (current)

| File | Change |
|---|---|
| [`AdapterFacade/Services/OrderAdapter.cs`](AdapterFacade/Services/OrderAdapter.cs) | **No functional change from the baseline.** The two resolvers still materialize into `List<OrderDto>` and return it. A short code comment in each resolver points at this plan file. No helpers added; no `using` lines added. |
| All other files | No change. |

## 7. Verification

`dotnet build AdapterFacade/AdapterFacade.csproj` → **Build succeeded. 0 Warning(s), 0 Error(s).**

The runtime error from the streaming attempt is fixed by the revert in step 6.

## 8. Open question for the user

Which of options A / C / D above should I implement? Option A is "ship the
reverted code as-is". Option C adds GraphQL subscriptions and is the cleanest
fit for `IAsyncEnumerable<OrderDto>`. Option D replaces the in-process
`IDocumentExecuter` with a hand-rolled selection-set walker over the gRPC
stream. My recommendation is **C** if the order source will ever return more
than ~10k rows per lookup, otherwise **A**.
