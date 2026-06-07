using AbonentClient;
using AdapterFacade.Services;
using OrderClient;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// Register gRPC clients for the data sources that the adapters wrap.
builder.Services.AddAbonentClient(builder.Configuration, "AbonentClient");
builder.Services.AddOrderClient(builder.Configuration, "OrderClient");

builder.Services.AddKeyedScoped<IAdapter, AbonentAdapter>(AbonentAdapter.SourceId);
builder.Services.AddKeyedScoped<IAdapter, OrderAdapter>(OrderAdapter.SourceId);

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<QueryService>();
app.MapGrpcReflectionService();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
