namespace Kakka.Service;

public interface IActor<TRequest, TResponse>
{
    Task<TResponse?> HandleRequestAsync(TRequest request);
}