using System.Collections.Concurrent;

namespace Kakka.Service;

public class ActorFactory<TRequest, TResponse>
{
    private readonly ConcurrentDictionary<string, IActor<TRequest, TResponse>> _actorCache = new();
    private readonly Func<string, IActor<TRequest, TResponse>> _actorCreator;

    public ActorFactory(Func<string, IActor<TRequest, TResponse>> actorCreator)
    {
        _actorCreator = actorCreator;
    }

    public IActor<TRequest, TResponse> Create(string actorId)
    {
        return _actorCache.GetOrAdd(actorId, _actorCreator);
    }
}