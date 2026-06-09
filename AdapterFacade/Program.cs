using AbonentClient;
using AdapterFacade.Services;
using HotChocolate;
using HotChocolate.Execution;
using OrderClient;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// Register gRPC clients for the data sources that the adapters wrap.
builder.Services.AddAbonentClient(builder.Configuration, "AbonentClient");
builder.Services.AddOrderClient(builder.Configuration, "OrderClient");

// Register the Hot Chocolate schema for OrderAdapter. The IRequestExecutor
// built here is what OrderAdapter consumes via DI — see
// plans/order-adapter-resolver-streaming-options.md §E.4.3.
builder.Services
    .AddGraphQL()
    .AddQueryType<OrderAdapter.OrderQuery>()
    .AddType<OrderAdapter.OrderDtoType>();

// Hot Chocolate 16 registers IRequestExecutorProvider in DI but does NOT
// register IRequestExecutor directly. OrderAdapter's constructor depends
// on IRequestExecutor, so we bridge the two by resolving the default
// executor once (lazily, on first keyed IAdapter resolution) and exposing
// it as a singleton. See plans/order-adapter-resolver-streaming-options.md
// §E.4.3 and §E.11.
builder.Services.AddSingleton<IRequestExecutor>(sp =>
    sp.GetRequiredService<IRequestExecutorProvider>()
        .GetExecutorAsync(ISchemaDefinition.DefaultName)
        .GetAwaiter()
        .GetResult());

// builder.Services.AddKeyedScoped<IAdapter, AbonentAdapter>(AbonentAdapter.SourceId);
builder.Services.AddKeyedScoped<IAdapter, OrderAdapter>(OrderAdapter.SourceId);

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<QueryService>();
app.MapGrpcReflectionService();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
