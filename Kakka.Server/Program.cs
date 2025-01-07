using Kakka.Service;
using Kakka.Service.Actors;

if (!File.Exists("client.properties"))
    Console.WriteLine("Missing Kafka configuration file \"client.properties\"");

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, eventArgs) =>
{
    Console.WriteLine("Cancellation requested...");
    // ReSharper disable once AccessToDisposedClosure
    cts.Cancel();
    eventArgs.Cancel = true; // Prevent immediate application exit
};

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

var actorFactory = new ActorFactory<string, int>(id => new RandomActor(id));
var kakkaClient = new KafkaKakkaClient<string, int>(id => actorFactory.Create(id));

_ = kakkaClient.Start(cts.Token);

app.MapGet("/test/{actor}", async (HttpContext context, string actor) =>
{
    var payload = DateTime.UtcNow.Millisecond.ToString();
    var reply = await kakkaClient.SendMessageAwaitReply(actor, payload, TimeSpan.FromSeconds(8));
    return Results.Ok(new { Actor = actor, Reply = reply });
});

Console.WriteLine("Application started. Press Ctrl+C to exit.");

await app.RunAsync(cts.Token);

Console.WriteLine("Application shutting down...");