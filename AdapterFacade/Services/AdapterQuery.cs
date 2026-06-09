namespace AdapterFacade.Services;

/// <summary>
/// Transport-agnostic carrier for a single GraphQL operation that an adapter
/// must execute. The adapter is responsible for selecting the correct root
/// field resolver and for applying the operation's selection set to the
/// streamed data. <see cref="Variables"/> is a string-keyed dictionary of
/// already-parsed JSON values (typically <see cref="string"/> for scalars);
/// <see cref="VariablesJson"/> is the original JSON payload for diagnostics.
/// </summary>
public sealed record AdapterQuery(
    string Query,
    string? OperationName,
    string? VariablesJson,
    IReadOnlyDictionary<string, object?>? Variables);
