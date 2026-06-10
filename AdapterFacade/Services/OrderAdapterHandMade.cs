using System.Runtime.CompilerServices;
using Grpc.Core;
using HotChocolate;
using HotChocolate.Execution;
using HotChocolate.Types;
using OrderClient;
using QuerySource;

namespace AdapterFacade.Services;

public class OrderAdapterHandMade : IAdapter
{
    public static string SourceId { get; } = "order_adapter_source_id";

    private readonly string _sdl;
    private readonly IRequestExecutor _executor;

    public OrderAdapterHandMade(IRequestExecutor requestExecutor)
    {
        _executor = requestExecutor;
        _sdl = _executor.Schema.ToString();
    }
    
    public async Task Find(AdapterQuery query, IServerStreamWriter<QueryResponse> responseStream, ServerCallContext context)
    {
        var hcRequestBuilder = OperationRequestBuilder.New()
            .SetDocument(query.Query);
        if (!string.IsNullOrWhiteSpace(query.OperationName))
        {
            hcRequestBuilder.SetOperationName(query.OperationName);
        }
        if (query.Variables is { Count: > 0 })
        {
            hcRequestBuilder.SetVariableValues(query.Variables);
        }
        
        var request = hcRequestBuilder.Build();
        
        var result = await _executor
            .ExecuteAsync(request, context.CancellationToken);

        Console.WriteLine(result.GetType().FullName);
        if (result is IResponseStream stream)
        {
            await foreach (var opResult in stream.ReadResultsAsync())
            {
                Console.WriteLine("opResult received");
            }
        }
    }

    public string Schema()
    {
        return _sdl;
    }
}


public class Order
{
    public string OrderId { get; set; }
    public string PhoneNumber { get; set; }
    public string ProductName { get; set; }
    public double Amount { get; set; }
}


public class Query
{
    [StreamResult]
    public async IAsyncEnumerable<Order> SearchOrders(
        [Service] IOrderClient orderClient,
        string phoneNumber,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var order in orderClient.GetOrdersByPhoneAsync(phoneNumber, cancellationToken))
        {
            Console.WriteLine($"return order {order.OrderId}");
            yield return new Order
            {
                OrderId = order.OrderId,
                PhoneNumber = order.PhoneNumber,
                ProductName = order.ProductName,
                Amount = order.Amount
            };
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
        }
    }
}
