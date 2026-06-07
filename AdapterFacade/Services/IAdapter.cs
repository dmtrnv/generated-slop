using Grpc.Core;
using QuerySource;

namespace AdapterFacade.Services;

public interface IAdapter
{
    /// <summary>
    /// Find data in bounded data source
    /// </summary>
    /// <param name="phoneNumbers">Phone numbers to search by</param>
    /// <param name="responseStream">Response stream</param>
    /// <param name="context">Call context</param>
    /// <returns>Streaming found data</returns>
    Task Find(
        IEnumerable<string> phoneNumbers,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context);

    /// <summary>
    /// Get GraphQL schema of result
    /// </summary>
    /// <returns>GraphQL schema of result</returns>
    string Schema();
}