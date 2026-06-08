using System.Text.Json;
using GraphQL;
using GraphQL.Types;
using GraphQL.Validation;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using QuerySource;

namespace AdapterFacade.Services;

public class QueryService(
        IServiceProvider serviceProvider,
        ILogger<QueryService> logger)
    : global::QuerySource.QueryService.QueryServiceBase
{
    private readonly IDocumentValidator _documentValidator = new DocumentValidator();

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

        // 2. Build a schema-first GraphQL schema from the SDL exposed by the
        //    adapter. We use GraphQL.NET for both query validation against
        //    this schema and for coercing the supplied variables.
        var schema = BuildSchema(adapter.Schema(), request.SourceId);
        var document = ParseQuery(request.Query);

        // 3. Parse the JSON variables string into a dictionary so GraphQL.NET
        //    can apply the operation's declared argument types to them.
        var rawVariables = ParseVariables(request.Variables);
        var coercedVariables = rawVariables.ToInputs();

        var validationResult = await _documentValidator.ValidateAsync(new ValidationOptions
        {
            Schema = schema,
            Document = document,
            Variables = coercedVariables,
            CancellationToken = context.CancellationToken,
        });

        if (!validationResult.IsValid)
        {
            var message = string.Join("; ", validationResult.Errors.Select(e => e.Message));
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, $"Query validation failed: {message}"));
        }

        // 4. Hand the validated operation over to the adapter. The adapter is
        //    now responsible for dispatching to the correct root field resolver
        //    and for applying the operation's selection set.
        var adapterQuery = new AdapterQuery(
            Query: request.Query,
            OperationName: string.IsNullOrWhiteSpace(request.OperationName) ? null : request.OperationName,
            VariablesJson: request.Variables,
            Variables: coercedVariables);

        await adapter.Find(adapterQuery, responseStream, context);
    }

    /// <summary>
    /// Builds a GraphQL.NET schema from the SDL string returned by an adapter
    /// using the schema-first (string-based) approach.
    /// </summary>
    private static Schema BuildSchema(string sdl, string sourceId)
    {
        if (string.IsNullOrWhiteSpace(sdl))
        {
            throw new RpcException(
                new Status(StatusCode.Internal, $"Adapter for source_id '{sourceId}' returned an empty schema"));
        }

        try
        {
            return Schema.For(sdl);
        }
        catch (Exception ex)
        {
            throw new RpcException(
                new Status(
                    StatusCode.Internal,
                    $"Failed to build schema for source_id '{sourceId}': {ex.Message}"));
        }
    }

    /// <summary>
    /// Parses the GraphQL query string into an AST document. Throws an
    /// <see cref="RpcException"/> with <see cref="StatusCode.InvalidArgument"/>
    /// if the query has syntax errors.
    /// </summary>
    private static GraphQLParser.AST.GraphQLDocument ParseQuery(string query)
    {
        try
        {
            return GraphQLParser.Parser.Parse(query);
        }
        catch (GraphQLParser.Exceptions.GraphQLSyntaxErrorException ex)
        {
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, $"Query syntax error: {ex.Message}"));
        }
    }

    /// <summary>
    /// Parses the JSON variables string into a
    /// <see cref="Dictionary{TKey,TValue}"/> that can be passed to
    /// <see cref="InputsExtensions.ToInputs"/> for further coercion by
    /// GraphQL.NET.
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
