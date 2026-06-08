using Grpc.Core;
using QuerySource;

namespace AdapterFacade.Services;

public interface IAdapter
{
    /// <summary>
    /// Executes the supplied GraphQL operation against the adapter's data
    /// source and streams the selection-set-projected results back to the
    /// caller. The adapter is responsible for dispatching to the correct
    /// root-field resolver and for honoring the operation's selection set.
    /// </summary>
    /// <param name="query">Carrier holding the query text, operation name, and (coerced) variables.</param>
    /// <param name="responseStream">Response stream.</param>
    /// <param name="context">Call context.</param>
    Task Find(
        AdapterQuery query,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context);

    /// <summary>
    /// Get GraphQL schema of result
    /// </summary>
    /// <returns>GraphQL schema of result</returns>
    string Schema();
}