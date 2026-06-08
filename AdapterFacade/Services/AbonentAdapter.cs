// using System.Text.Json;
// using AbonentClient;
// using AbonentSource;
// using Grpc.Core;
// using QuerySource;
//
// namespace AdapterFacade.Services;
//
// public class AbonentAdapter : IAdapter
// {
//     public static string SourceId { get; } = "abonent_adapter_source_id";
//
//     private readonly IAbonentClient _abonentClient;
//     private readonly ILogger<AbonentAdapter> _logger;
//
//     public AbonentAdapter(IAbonentClient abonentClient, ILogger<AbonentAdapter> logger)
//     {
//         _abonentClient = abonentClient ?? throw new ArgumentNullException(nameof(abonentClient));
//         _logger = logger ?? throw new ArgumentNullException(nameof(logger));
//     }
//
//     public async Task Find(
//         IEnumerable<string> phoneNumbers,
//         IServerStreamWriter<QueryResponse> responseStream,
//         ServerCallContext context)
//     {
//         ArgumentNullException.ThrowIfNull(phoneNumbers);
//         ArgumentNullException.ThrowIfNull(responseStream);
//         ArgumentNullException.ThrowIfNull(context);
//
//         var schema = Schema();
//
//         foreach (var phoneNumber in phoneNumbers)
//         {
//             if (string.IsNullOrWhiteSpace(phoneNumber))
//             {
//                 _logger.LogWarning("Skipping blank phone number in AbonentAdapter.Find");
//                 continue;
//             }
//
//             if (context.CancellationToken.IsCancellationRequested)
//             {
//                 break;
//             }
//
//             _logger.LogInformation("AbonentAdapter searching for phone {Phone}", phoneNumber);
//
//             try
//             {
//                 await foreach (var abonent in _abonentClient
//                     .GetAbonentsByPhoneAsync(phoneNumber, context.CancellationToken)
//                     .WithCancellation(context.CancellationToken))
//                 {
//                     if (context.CancellationToken.IsCancellationRequested)
//                     {
//                         break;
//                     }
//
//                     var data = SerializeAbonent(abonent);
//
//                     _logger.LogInformation(
//                         "Streaming abonent {AbonentId} ({Name}) for phone {Phone}",
//                         abonent.AbonentId,
//                         abonent.Name,
//                         abonent.PhoneNumber);
//
//                     await responseStream.WriteAsync(new QueryResponse
//                     {
//                         ResultSchema = schema,
//                         Data = data,
//                     });
//                 }
//             }
//             catch (OperationCanceledException)
//             {
//                 throw;
//             }
//             catch (Exception ex)
//             {
//                 _logger.LogError(
//                     ex,
//                     "Failed to fetch abonents for phone {Phone} from AbonentAdapter",
//                     phoneNumber);
//                 throw new RpcException(
//                     new Status(StatusCode.Internal, $"Failed to fetch abonents: {ex.Message}"));
//             }
//         }
//     }
//
//     public string Schema()
//     {
//         // GraphQL SDL describing the Abonent entity produced by this adapter.
//         // Mirrors the fields declared in Protos/abonent.proto (AbonentInfo):
//         //   - abonent_id   (string)
//         //   - phone_number (string)
//         //   - name         (string)
//         return """
//         type Abonent {
//             abonent_id: String!
//             phone_number: String!
//             name: String!
//         }
//
//         type Query {
//             searhByPhoneNumber(phone_number: String!): [Abonent!]!
//         }
//         """;
//     }
//
//     private static string SerializeAbonent(AbonentInfo abonent)
//     {
//         var payload = new
//         {
//             abonent_id = abonent.AbonentId,
//             phone_number = abonent.PhoneNumber,
//             name = abonent.Name,
//         };
//
//         return JsonSerializer.Serialize(payload);
//     }
// }
