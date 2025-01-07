namespace Kakka.Service.Actors;

public class RandomActor : IActor<string, int>
{
    private readonly string _actorId;
    public RandomActor(string actorId)
    {
        Console.WriteLine($"Instantiating actor {actorId}");
        _actorId = actorId;
    }

    public Task<int> HandleRequestAsync(string request)
    {
        Console.WriteLine($"Actor {_actorId} handling request");
        return Task.FromResult(new Random().Next());
    }
}