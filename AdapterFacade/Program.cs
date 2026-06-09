using AbonentClient;
using AdapterFacade.Services;
using GraphQL;
using GraphQL.Execution;
using GraphQL.Validation;
using GraphQLParser.AST;
using OrderClient;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// Register gRPC clients for the data sources that the adapters wrap.
builder.Services.AddAbonentClient(builder.Configuration, "AbonentClient");
builder.Services.AddOrderClient(builder.Configuration, "OrderClient");

// Plan C — register a single IDocumentExecuter with an
// ExecutionStrategySelector that maps OperationType.Subscription to
// SubscriptionExecutionStrategy (the only strategy in GraphQL.NET 8 that
// natively supports IAsyncEnumerable<T> resolvers). Query and Mutation
// continue to use the default serial strategy.
// See plans/order-adapter-resolver-streaming-options.md §C.3.5.
builder.Services.AddSingleton<SubscriptionExecutionStrategy>();
builder.Services.AddSingleton<IDocumentExecuter>(sp =>
{
    var registrations = new ExecutionStrategyRegistration[]
    {
        // Query and Mutation keep the default serial behaviour.
        new(new SerialExecutionStrategy(), OperationType.Query),
        new(new SerialExecutionStrategy(), OperationType.Mutation),
        // Subscription routes through the strategy that knows how to
        // iterate IAsyncEnumerable<T> resolvers per source event.
        new(sp.GetRequiredService<SubscriptionExecutionStrategy>(), OperationType.Subscription),
    };
    var selector = new DefaultExecutionStrategySelector(registrations);
    return new DocumentExecuter(
        new GraphQLDocumentBuilder(),
        new DocumentValidator(),
        selector,
        Enumerable.Empty<GraphQL.DI.IConfigureExecution>());
});

// builder.Services.AddKeyedScoped<IAdapter, AbonentAdapter>(AbonentAdapter.SourceId);
builder.Services.AddKeyedScoped<IAdapter, OrderAdapter>(OrderAdapter.SourceId);

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<QueryService>();
app.MapGrpcReflectionService();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
