using GraphQL;

namespace AdapterFacade.Services;

/// <summary>
/// Transport-agnostic carrier for a single GraphQL operation that an adapter
/// must execute. The adapter is responsible for selecting the correct root
/// field resolver and for applying the operation's selection set to the
/// streamed data — <see cref="Variables"/> are the GraphQL.NET-coerced
/// variables, <see cref="VariablesJson"/> is the original JSON for diagnostics.
/// </summary>
public sealed record AdapterQuery(
    string Query,
    string? OperationName,
    string? VariablesJson,
    Inputs? Variables);
