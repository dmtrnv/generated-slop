

c# hotchocolate use custom request executor inside specific service class - so theres like local graphql server (local to current service class)

---


Yes — in Hot Chocolate you can create and use a **separate `IRequestExecutor` instance inside a service class**, effectively acting like a **local/in-memory GraphQL server** scoped to that service instead of the main ASP.NET GraphQL endpoint.

The idea is:

- Build a dedicated schema for that service
- Get an `IRequestExecutorResolver`
- Resolve a named executor (or build one manually)
- Execute GraphQL requests programmatically

### Option 1: Named executor (recommended)

Register a second GraphQL schema with a name.

```
builder.Services
    .AddGraphQLServer("LocalExecutor")
    .AddQueryType<LocalQuery>()
    .AddMutationType<LocalMutation>();
```

Then inject `IRequestExecutorResolver` into your service:

```
using HotChocolate.Execution;

public class MyService
{
    private readonly IRequestExecutorResolver _executorResolver;

    public MyService(IRequestExecutorResolver executorResolver)
    {
        _executorResolver = executorResolver;
    }

    public async Task<string?> ExecuteSomething()
    {
        var executor =
            await _executorResolver.GetRequestExecutorAsync("LocalExecutor");

        IExecutionResult result = await executor.ExecuteAsync(@"
        query {
            hello
        }");

        var json = await result.ToJsonAsync();

        return json;
    }
}
```

Example local schema:

```
public class LocalQuery
{
    public string Hello() => "hello from local graphql";
}
```

This gives you a **completely separate GraphQL runtime** from your public API.

---

### Option 2: Build a fully isolated executor inside the service

If you want a GraphQL executor that is **only local to one service class** and not registered globally in DI:

```
using HotChocolate.Execution.Configuration;
using Microsoft.Extensions.DependencyInjection;

public class MyService
{
    private readonly IRequestExecutor _executor;

    public MyService()
    {
        var services = new ServiceCollection();

        _executor = services
            .AddGraphQL()
            .AddQueryType<LocalQuery>()
            .BuildRequestExecutorAsync()
            .GetAwaiter()
            .GetResult();
    }

    public async Task<string> Run()
    {
        var result = await _executor.ExecuteAsync("""
        query {
            hello
        }
        """);

        return await result.ToJsonAsync();
    }
}
```

This is basically an **embedded GraphQL engine** that exists only inside that service.

However, there are tradeoffs:

- You lose normal app DI unless you wire it in manually
- Schema compilation can be expensive
- You should create it once and reuse it, not per request

---

### Option 3: Execute resolvers without HTTP but with app DI

Often the best architecture is:

```
builder.Services
    .AddGraphQLServer("Internal")
    .AddQueryType<InternalQuery>();
```

Then:

```
public class PricingService
{
    private readonly IRequestExecutor _executor;

    public PricingService(
        IRequestExecutorResolver resolver)
    {
        _executor = resolver
            .GetRequestExecutorAsync("Internal")
            .GetAwaiter()
            .GetResult();
    }

    public async Task<PriceDto> Calculate()
    {
        var result = await _executor.ExecuteAsync("""
        query {
            calculatePrice(productId: 1) {
                amount
            }
        }
        """);

        var queryResult = result.ExpectQueryResult();

        return queryResult
            .Data!["calculatePrice"]
            .ToObject<PriceDto>();
    }
}
```

This works nicely if your service wants to reuse GraphQL business logic/resolvers internally.

### One architectural warning

If your goal is just to reuse resolver logic, avoid:

> service → graphql executor → resolver → service

because it can become circular and hard to debug.

Usually better:

```
resolver
   ↓
domain service
```

and your internal service calls the **same domain service directly**.

Use an internal executor when you specifically want:

- dynamic GraphQL queries
- schema-driven composition
- plugin/module execution
- federation-like local orchestration
- field selection/projection behavior

For “reuse resolver code”, direct service calls are typically cleaner.