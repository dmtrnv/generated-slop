using Grpc.Core;

namespace AbonentSource.Services;

public class AbonentInfoService(ILogger<AbonentInfoService> logger) : AbonentService.AbonentServiceBase
{
    private static readonly List<AbonentInfo> Abonents = new()
    {
        new AbonentInfo { AbonentId = "AB-1001", PhoneNumber = "+79991234567", Name = "Ivan Petrov" },
        new AbonentInfo { AbonentId = "AB-1002", PhoneNumber = "+79991234567", Name = "Maria Petrova" },
        new AbonentInfo { AbonentId = "AB-2001", PhoneNumber = "+79997654321", Name = "Sergey Smirnov" },
        new AbonentInfo { AbonentId = "AB-3001", PhoneNumber = "+79165555555", Name = "Anna Kuznetsova" },
        new AbonentInfo { AbonentId = "AB-3002", PhoneNumber = "+79165555555", Name = "Pavel Kuznetsov" },
        new AbonentInfo { AbonentId = "AB-3003", PhoneNumber = "+79165555555", Name = "Olga Sokolova" },
    };

    public override async Task GetAbonentsByPhone(
        PhoneRequest request,
        IServerStreamWriter<AbonentInfo> responseStream,
        ServerCallContext context)
    {
        logger.LogInformation("GetAbonentsByPhone called with phone: {Phone}", request.PhoneNumber);

        var matches = Abonents
            .Where(a => string.Equals(a.PhoneNumber, request.PhoneNumber, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (matches.Count == 0)
        {
            logger.LogInformation("No abonents found for phone {Phone}", request.PhoneNumber);
            return;
        }

        foreach (var abonent in matches)
        {
            // Emulate some processing time for streaming demonstration.
            await Task.Delay(200, context.CancellationToken);

            if (context.CancellationToken.IsCancellationRequested)
            {
                break;
            }

            logger.LogInformation(
                "Streaming abonent {AbonentId} ({Name}) for phone {Phone}",
                abonent.AbonentId,
                abonent.Name,
                abonent.PhoneNumber);

            await responseStream.WriteAsync(abonent);
        }
    }
}
