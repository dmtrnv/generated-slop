using System.Text.Json;
using GraphQL;
using GraphQL.Types;
using GraphQL.Validation;
using GraphQLParser.AST;
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

        // 4. Pull the requested phone numbers out of the coerced variables.
        //    After successful validation GraphQL.NET has coerced the
        //    dictionary into the types declared by the operation.
        var phoneNumbers = ExtractPhoneNumbers(coercedVariables);
        if (phoneNumbers.Count == 0)
        {
            logger.LogWarning(
                "No phone numbers resolved from query variables for source {SourceId}; nothing to stream",
                request.SourceId);
            return;
        }

        // 5. Extract the selection set of the top-level query field and the
        //    directives applied to it. These are forwarded to the adapter
        //    so it can shape the streamed schema and the resulting data
        //    payload to match what the client actually asked for.
        var (selectionSet, directives) = ExtractTopLevelSelection(document);

        // 6. Delegate the actual data fetch / streaming to the resolved adapter.
        await adapter.Find(phoneNumbers, selectionSet, directives, responseStream, context);
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
    private static GraphQLDocument ParseQuery(string query)
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
            return JsonSerializer.Deserialize<Dictionary<string, object?>>(variables, JsonOptions)
                ?? new Dictionary<string, object?>();
        }
        catch (JsonException ex)
        {
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, $"Invalid variables JSON: {ex.Message}"));
        }
    }

    /// <summary>
    /// Extracts the phone numbers from the coerced query variables produced
    /// by GraphQL.NET. Supports either <c>phone_numbers</c> (list) or
    /// <c>phone_number</c> (single string) variable names, mirroring the
    /// field names declared in the adapter schemas.
    /// </summary>
    private static List<string> ExtractPhoneNumbers(Inputs variables)
    {
        if (variables.TryGetValue("phone_numbers", out var list) && list is IEnumerable<object?> many)
        {
            var result = new List<string>();
            foreach (var v in many)
            {
                if (v is string s && !string.IsNullOrWhiteSpace(s))
                {
                    result.Add(s);
                }
            }
            return result;
        }

        if (variables.TryGetValue("phone_number", out var single)
            && single is not null
            && !string.IsNullOrWhiteSpace(single.ToString()))
        {
            var value = single.ToString();
            if (value is not null)
            {
                return [value];
            }
        }

        return [];
    }

    /// <summary>
    /// Walks the parsed GraphQL document and returns:
    ///   - the list of fields selected inside the first list-typed
    ///     selection of the top-level query field (e.g. the inner
    ///     fields of <c>searhByPhoneNumber { ... }</c>), in document
    ///     order; and
    ///   - the <see cref="AppliedDirective"/>s applied to that same
    ///     top-level field, in document order.
    ///
    /// If no list-typed selection is present, the inner selection list
    /// is empty and the directives are taken from the first field in
    /// the document.
    /// </summary>
    private static (IReadOnlyList<GraphQLField> SelectionSet, IReadOnlyList<AppliedDirective> Directives)
        ExtractTopLevelSelection(GraphQLDocument document)
    {
        var operation = document.Definitions.OfType<GraphQLOperationDefinition>().FirstOrDefault();
        if (operation is null)
        {
            return (Array.Empty<GraphQLField>(), Array.Empty<AppliedDirective>());
        }

        var topLevelField = operation.SelectionSet.Selections.OfType<GraphQLField>().FirstOrDefault();
        if (topLevelField is null)
        {
            return (Array.Empty<GraphQLField>(), Array.Empty<AppliedDirective>());
        }

        var directives = ToAppliedDirectives(topLevelField.Directives);

        // The data streamed back by adapters is shaped like the inner
        // fields of the list-returning field (e.g. Order / Abonent),
        // so we surface that selection set - in document order.
        var innerField = topLevelField.SelectionSet?.Selections.OfType<GraphQLField>().FirstOrDefault();
        if (innerField is null || innerField.SelectionSet is null)
        {
            return (Array.Empty<GraphQLField>(), directives);
        }

        var fields = new List<GraphQLField>(innerField.SelectionSet.Selections.Count);
        foreach (var selection in innerField.SelectionSet.Selections)
        {
            if (selection is GraphQLField field)
            {
                fields.Add(field);
            }
        }
        return (fields, directives);
    }

    /// <summary>
    /// Converts the directives from a GraphQLParser AST node into
    /// GraphQL.NET <see cref="AppliedDirective"/> instances, preserving
    /// the order in which they were written in the document.
    /// </summary>
    private static IReadOnlyList<AppliedDirective> ToAppliedDirectives(GraphQLDirectives? directives)
    {
        if (directives is null || directives.Count == 0)
        {
            return Array.Empty<AppliedDirective>();
        }

        var result = new List<AppliedDirective>(directives.Count);
        foreach (var directive in directives.Items)
        {
            var applied = new AppliedDirective(directive.Name.StringValue);
            if (directive.Arguments is { Count: > 0 } args)
            {
                foreach (var arg in args.Items)
                {
                    applied.AddArgument(new DirectiveArgument(arg.Name.StringValue)
                    {
                        Value = arg.Value.ToString(),
                    });
                }
            }
            result.Add(applied);
        }
        return result;
    }
}
