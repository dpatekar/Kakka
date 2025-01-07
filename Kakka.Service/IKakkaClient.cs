namespace Kakka.Service;

public interface IKakkaClient<TRequest, TResponse>
{
    Task Start(CancellationToken cancellationToken);
    
    Task<TResponse?> SendMessageAwaitReply(
        string actorId,
        TRequest requestObj,
        TimeSpan? timeout = null);
}