using System.Text.Json;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using QuerySource;

namespace AdapterFacade.Services;

public class QueryService(
        IServiceProvider serviceProvider,
        ILogger<QueryService> logger)
    : global::QuerySource.QueryService.QueryServiceBase
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public override async Task ExecuteQuery(
        QueryRequest request,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        logger.LogInformation(
            "ExecuteQuery called with source_id: {SourceId}, query length: {QueryLength}, variables: {Variables}",
            request.SourceId,
            request.Query?.Length ?? 0,
            string.IsNullOrEmpty(request.Variables) ? "<none>" : "<provided>");

        if (string.IsNullOrWhiteSpace(request.SourceId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "source_id must be provided"));
        }

        if (string.IsNullOrWhiteSpace(request.Query))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "query must be provided"));
        }

        // 1. Resolve the adapter registered for the requested source id.
        var adapter = serviceProvider.GetKeyedService<IAdapter>(request.SourceId);
        if (adapter is null)
        {
            throw new RpcException(
                new Status(StatusCode.NotFound, $"No adapter registered for source_id '{request.SourceId}'"));
        }

        // 2. Parse the JSON variables string into a dictionary. We pass the
        //    already-parsed values to the adapter; the adapter (Hot Chocolate
        //    in our case) is responsible for type coercion against the schema.
        var rawVariables = ParseVariables(request.Variables);

        // 3. Hand the operation over to the adapter. The adapter now owns
        //    validation, execution, and streaming back to the responseStream.
        var adapterQuery = new AdapterQuery(
            Query: request.Query,
            OperationName: string.IsNullOrWhiteSpace(request.OperationName) ? null : request.OperationName,
            VariablesJson: request.Variables,
            Variables: rawVariables);

        await adapter.Find(adapterQuery, responseStream, context);
    }

    /// <summary>
    /// Parses the JSON variables string into a
    /// <see cref="Dictionary{TKey,TValue}"/> that can be passed to the adapter.
    /// For the Hot Chocolate adapter the dictionary values are forwarded
    /// verbatim into the executor's <c>OperationRequest.Variables</c>.
    /// </summary>
    private static Dictionary<string, object?> ParseVariables(string? variables)
    {
        if (string.IsNullOrWhiteSpace(variables))
        {
            return new Dictionary<string, object?>();
        }

        try
        {
            var result = JsonSerializer.Deserialize<Dictionary<string, object?>>(variables, JsonOptions)
                ?? new Dictionary<string, object?>();
            foreach (var pair in result)
            {
                result[pair.Key] = pair.Value?.ToString();
            }

            return result;
        }
        catch (JsonException ex)
        {
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, $"Invalid variables JSON: {ex.Message}"));
        }
    }
}
