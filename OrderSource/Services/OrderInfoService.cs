using Grpc.Core;

namespace OrderSource.Services;

public class OrderInfoService(ILogger<OrderInfoService> logger) : OrderService.OrderServiceBase
{
    private static readonly List<OrderInfo> Orders = new()
    {
        new OrderInfo { OrderId = "ORD-1001", PhoneNumber = "+79991234567", ProductName = "Laptop Lenovo IdeaPad 5", Amount = 89999.99 },
        new OrderInfo { OrderId = "ORD-1002", PhoneNumber = "+79991234567", ProductName = "Wireless Mouse Logitech MX Master 3", Amount = 7490.00 },
        new OrderInfo { OrderId = "ORD-1003", PhoneNumber = "+79991234567", ProductName = "USB-C Hub", Amount = 3290.50 },
        new OrderInfo { OrderId = "ORD-2001", PhoneNumber = "+79997654321", ProductName = "Smartphone Samsung Galaxy S24", Amount = 74990.00 },
        new OrderInfo { OrderId = "ORD-2002", PhoneNumber = "+79997654321", ProductName = "Protective Case", Amount = 1290.00 },
        new OrderInfo { OrderId = "ORD-3001", PhoneNumber = "+79165555555", ProductName = "Mechanical Keyboard Keychron K2", Amount = 11990.00 },
    };

    public override async Task GetOrdersByPhone(
        PhoneRequest request,
        IServerStreamWriter<OrderInfo> responseStream,
        ServerCallContext context)
    {
        logger.LogInformation("GetOrdersByPhone called with phone: {Phone}", request.PhoneNumber);

        var matches = Orders
            .Where(o => string.Equals(o.PhoneNumber, request.PhoneNumber, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (matches.Count == 0)
        {
            logger.LogInformation("No orders found for phone {Phone}", request.PhoneNumber);
            return;
        }

        foreach (var order in matches)
        {
            // Emulate some processing time for streaming demonstration.
            await Task.Delay(200, context.CancellationToken);

            if (context.CancellationToken.IsCancellationRequested)
            {
                break;
            }

            logger.LogInformation(
                "Streaming order {OrderId} for phone {Phone}",
                order.OrderId,
                order.PhoneNumber);

            await responseStream.WriteAsync(order);
            await Task.Delay(TimeSpan.FromSeconds(3), context.CancellationToken);
        }
    }

    public override async Task GetOrdersByOrderId(
        OrderIdRequest request,
        IServerStreamWriter<OrderInfo> responseStream,
        ServerCallContext context)
    {
        logger.LogInformation("GetOrdersByOrderId called with order_id: {OrderId}", request.OrderId);

        var matches = Orders
            .Where(o => string.Equals(o.OrderId, request.OrderId, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (matches.Count == 0)
        {
            logger.LogInformation("No orders found for order_id {OrderId}", request.OrderId);
            return;
        }

        foreach (var order in matches)
        {
            // Emulate some processing time for streaming demonstration.
            await Task.Delay(200, context.CancellationToken);

            if (context.CancellationToken.IsCancellationRequested)
            {
                break;
            }

            logger.LogInformation(
                "Streaming order {OrderId} (matched by order_id)",
                order.OrderId);

            await responseStream.WriteAsync(order);
        }
    }
}
