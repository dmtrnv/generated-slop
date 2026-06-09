using GraphQL.Types;
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
    /// The adapter's code-first GraphQL.NET <see cref="ISchema"/> instance,
    /// including all resolvers and <c>StreamResolver</c>s. This is the
    /// authoritative schema used for query validation and execution — it
    /// preserves the runtime configuration (e.g. subscription
    /// <c>StreamResolver</c>s) that cannot be round-tripped through SDL.
    /// </summary>
    ISchema GraphQLSchema { get; }

    /// <summary>
    /// Printable SDL representation of the adapter's schema, used as the
    /// gRPC <c>ResultSchema</c> carrier value. Derived from
    /// <see cref="GraphQLSchema"/>.
    /// </summary>
    /// <returns>GraphQL schema SDL of result</returns>
    string Schema();
}