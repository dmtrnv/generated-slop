using System.Text.Json;
using GraphQLParser;
using GraphQLParser.AST;
using GraphQLParser.Visitors;

namespace AdapterFacade.Services;

/// <summary>
/// Identifies the entity type produced by an adapter. The actual field
/// definitions (names and GraphQL types) are taken from the adapter's
/// own SDL string, which is the single source of truth for the
/// entity's shape.
/// </summary>
public sealed class EntityTypeDefinition
{
    /// <summary>
    /// The GraphQL type name (e.g. <c>"Order"</c>). Must match the
    /// <c>type <Name> { ... }</c> definition declared in the
    /// adapter's SDL.
    /// </summary>
    public string GraphQLTypeName { get; }

    public EntityTypeDefinition(string graphqlTypeName)
    {
        if (string.IsNullOrWhiteSpace(graphqlTypeName))
        {
            throw new ArgumentException("GraphQL type name must be provided", nameof(graphqlTypeName));
        }

        GraphQLTypeName = graphqlTypeName;
    }
}

/// <summary>
/// Generic helpers for adapters that stream a list of records back to
/// the client. The helpers:
///   - rewrite the adapter's existing GraphQL SDL in-place by mutating
///     the parsed <see cref="GraphQLDocument"/> (so the streamed
///     response declares exactly the fields the client selected,
///     <b>in the order they were written</b>);
///   - serialize each record to JSON, including only the selected
///     fields (object property order is not significant).
/// The adapter's <c>Schema()</c> string is the single source of truth
/// for the entity's field types; <see cref="EntityTypeDefinition"/>
/// only names the entity itself.
/// </summary>
public static class SelectionSetSerializer
{
    /// <summary>
    /// Rewrites the supplied SDL by replacing the body of the entity
    /// type definition with the fields the client selected (in
    /// document order). All other type definitions in the SDL
    /// (notably the <c>Query</c> type) are left untouched. Field
    /// types are taken directly from the original SDL - the caller
    /// does not need to (and cannot) supply them separately.
    /// </summary>
    /// <param name="originalSchema">
    /// The full SDL returned by the adapter. Must contain a
    /// <c>type <Name> { ... }</c> definition whose name matches
    /// <paramref name="entityType"/>.
    /// </param>
    /// <param name="entityType">The entity type being returned.</param>
    /// <param name="selectionSet">
    /// The fields the client selected, in document order. Fields not
    /// declared on the entity in <paramref name="originalSchema"/> are
    /// skipped silently.
    /// </param>
    public static string RewriteSchema(
        string originalSchema,
        EntityTypeDefinition entityType,
        IReadOnlyList<GraphQLField> selectionSet)
    {
        ArgumentNullException.ThrowIfNull(originalSchema);
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(selectionSet);

        // Parse the original SDL into a GraphQLDocument so we can
        // operate on a structured representation rather than on raw
        // text. We use the parsed entity type's own field types as
        // the source of truth, then splice the selected fields into
        // the document and re-print it.
        var document = Parser.Parse(originalSchema);
        var objectType = FindObjectTypeDefinition(document, entityType.GraphQLTypeName)
            ?? throw new InvalidOperationException(
                $"Entity type '{entityType.GraphQLTypeName}' not found in adapter SDL.");

        // Ensure the type has a fields definition we can mutate. It
        // should always be non-null for an object type definition
        // produced by the parser, but guard anyway.
        if (objectType.Fields is null)
        {
            throw new InvalidOperationException(
                $"Type '{entityType.GraphQLTypeName}' has no fields definition in adapter SDL.");
        }

        // Index the entity's declared fields by name so we can look
        // up the original GraphQLType for each selected field. The
        // types come straight from the parsed SDL - no separate field
        // type dictionary needs to be maintained.
        var declaredFields = new Dictionary<string, GraphQLType>(StringComparer.Ordinal);
        foreach (var declaredField in objectType.Fields.Items)
        {
            if (declaredField.Type is null)
            {
                continue;
            }
            declaredFields[declaredField.Name.StringValue] = declaredField.Type;
        }

        // Build the new field list in selection-set (document) order,
        // skipping any field that is not declared on the entity in
        // the original SDL. This preserves "only the fields the
        // client asked for" without losing type fidelity.
        var newFields = new List<GraphQLFieldDefinition>(selectionSet.Count);
        foreach (var field in selectionSet)
        {
            var name = field.Name.StringValue;
            if (!declaredFields.TryGetValue(name, out var fieldType))
            {
                continue;
            }

            newFields.Add(new GraphQLFieldDefinition(
                new GraphQLName(name),
                fieldType));
        }

        // Splice the new fields into the parsed document. The Query
        // type and any other type definitions are part of the same
        // document and are therefore preserved untouched when we
        // re-print the document below.
        objectType.Fields.Items.Clear();
        foreach (var newField in newFields)
        {
            objectType.Fields.Items.Add(newField);
        }

        return new SDLPrinter().Print(document);
    }

    /// <summary>
    /// Serializes a record to JSON, including only the fields the
    /// client selected. Object property order is not significant.
    /// </summary>
    /// <param name="selectionSet">Fields selected by the client, in document order.</param>
    /// <param name="valueProvider">
    /// Resolver that returns the value of the named field on
    /// <paramref name="record"/>, or <c>null</c> if the field is not
    /// applicable to this record (the field is then omitted from the
    /// payload).
    /// </param>
    /// <param name="record">The record being serialized.</param>
    public static string Serialize<T>(
        IReadOnlyList<GraphQLField> selectionSet,
        T record,
        Func<T, string, object?> valueProvider)
    {
        ArgumentNullException.ThrowIfNull(selectionSet);
        ArgumentNullException.ThrowIfNull(valueProvider);
        ArgumentNullException.ThrowIfNull(record);

        var dict = new Dictionary<string, object?>(selectionSet.Count);

        foreach (var field in selectionSet)
        {
            var name = field.Name.StringValue;
            if (dict.ContainsKey(name))
            {
                // Field is selected more than once (e.g. via alias);
                // the first occurrence wins.
                continue;
            }

            // Returning null from the value provider means "this field
            // is not part of the entity" - the field is omitted from
            // the payload. Non-null values are emitted as-is.
            var value = valueProvider(record, name);
            if (value is not null)
            {
                dict[name] = value;
            }
        }

        return JsonSerializer.Serialize(dict);
    }

    private static GraphQLObjectTypeDefinition? FindObjectTypeDefinition(
        GraphQLDocument document, string typeName)
    {
        foreach (var definition in document.Definitions)
        {
            if (definition is GraphQLObjectTypeDefinition objectType
                && string.Equals(objectType.Name.StringValue, typeName, StringComparison.Ordinal))
            {
                return objectType;
            }
        }
        return null;
    }
}
