using GraphQL.Types;
using GraphQLParser.AST;
using Grpc.Core;
using QuerySource;

namespace AdapterFacade.Services;

public interface IAdapter
{
    /// <summary>
    /// Find data in bounded data source.
    /// </summary>
    /// <param name="phoneNumbers">Phone numbers to search by</param>
    /// <param name="selectionSet">
    /// Fields the caller selected in the GraphQL query, in the order they
    /// were written in the document. The returned data must include
    /// exactly these fields (object field order is not significant), and
    /// the schema streamed back to the client must declare the same
    /// fields in the same order.
    /// </param>
    /// <param name="directives">
    /// Directives applied to the top-level selection (i.e. the
    /// <c>searhByPhoneNumber</c> field in the GraphQL query). Adapters
    /// may interpret them; this layer simply forwards them as-is.
    /// </param>
    /// <param name="responseStream">Response stream</param>
    /// <param name="context">Call context</param>
    /// <returns>Streaming found data</returns>
    Task Find(
        IEnumerable<string> phoneNumbers,
        IReadOnlyList<GraphQLField> selectionSet,
        IReadOnlyList<AppliedDirective> directives,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context);

    /// <summary>
    /// Get GraphQL schema of result.
    /// </summary>
    /// <returns>GraphQL schema of result</returns>
    string Schema();
}
