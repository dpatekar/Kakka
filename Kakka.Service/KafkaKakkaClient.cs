using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kakka.Service.Kafka;
using Microsoft.Extensions.Configuration;
using Partitioner = Confluent.Kafka.Partitioner;

namespace Kakka.Service;

public class KafkaKakkaClient<TRequest, TResponse> : IKakkaClient<TRequest, TResponse>
{
    private const string ActorTopic = "actor_topic";
    private readonly string ReplyTopic;
    private const int PartitionCount = 256;
    private const string ClientIdFile = "clientid.txt";

    private readonly IProducer<string, string> _producer;
    private readonly IConsumer<string, string> _actorConsumer;
    private readonly IConsumer<string, string> _replyConsumer;
    private readonly Dictionary<string, string?> _kafkaConfig;

    /// <summary>
    /// Tracks in-flight requests by correlationId, completing with a typed TResponse.
    /// </summary>
    private readonly ConcurrentDictionary<string, TaskCompletionSource<TResponse?>> _pendingReplies
        = new();

    /// <summary>
    /// A user-supplied factory to create or retrieve an IActor for a given actorId.
    /// </summary>
    private readonly Func<string, IActor<TRequest, TResponse>> _actorFactory;

    public KafkaKakkaClient(Func<string, IActor<TRequest, TResponse>> actorFactory)
    {
        // 1) Keep reference to the actor factory
        _actorFactory = actorFactory;

        // 2) Load Kafka config from an INI file
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddIniFile("client.properties", optional: false)
            .Build();

        _kafkaConfig = config.AsEnumerable().ToDictionary(k => k.Key, v => v.Value);

        // 3) Get/create a stable clientId for this instance
        var clientId = GetOrCreateClientId(ClientIdFile);

        // 4) Dynamic reply topic based on clientId
        ReplyTopic = $"actor_reply_{clientId}";

        // 5) Build consumer configs
        var actorConsumerConfig = new ConsumerConfig(_kafkaConfig)
        {
            GroupId = "actor-group",
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        var replyConsumerConfig = new ConsumerConfig(_kafkaConfig)
        {
            GroupId = clientId,
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        // 6) Create consumers
        _actorConsumer = new ConsumerBuilder<string, string>(actorConsumerConfig).Build();
        _replyConsumer = new ConsumerBuilder<string, string>(replyConsumerConfig).Build();

        // 7) Create producer
        var producerConfig = new ProducerConfig(_kafkaConfig)
        {
            Partitioner = Partitioner.Murmur2 // consistent partitioning by key
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    /// <summary>
    /// Start both consumers:
    /// - actor-topic consumer (handling requests via IActor),
    /// - reply-topic consumer (handling replies for our own requests).
    /// </summary>
    public async Task Start(CancellationToken cancellationToken)
    {
        // Ensure topics exist
        await EnsureTopicsExist();
        // Give the cluster a moment to propagate new-topic metadata
        await Task.Delay(2000);

        // 1) Actor consumer: listens for TRequest messages, calls the appropriate IActor,
        //    and publishes TResponse to the reply topic.
        var actorTask = Task.Run(async () =>
        {
            Console.WriteLine("Subscribing to actor-topic");
            _actorConsumer.Subscribe(ActorTopic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = _actorConsumer.Consume(cancellationToken);
                    if (consumeResult == null)
                        continue;

                    var actorId = consumeResult.Message.Key; // partition key
                    var rawRequestJson = consumeResult.Message.Value;

                    // Extract correlationId from headers (if any)
                    var correlationHeader = consumeResult.Message.Headers
                        .FirstOrDefault(h => h.Key == "correlationId");
                    var correlationId = correlationHeader != null
                        ? Encoding.UTF8.GetString(correlationHeader.GetValueBytes()!)
                        : null;

                    Console.WriteLine($"[ActorConsumer] Request arrived. ActorId={actorId}, CorrId={correlationId}");

                    // Deserialize the request object
                    TRequest? requestObj = default;
                    try
                    {
                        requestObj = JsonSerializer.Deserialize<TRequest>(rawRequestJson);
                        if (requestObj == null)
                        {
                            Console.WriteLine($"[ActorConsumer] Deserialization returned null TRequest. JSON={rawRequestJson}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ActorConsumer] Error deserializing TRequest: {ex}");
                    }

                    if (requestObj == null)
                    {
                        // Can't process further if the request is null
                        continue;
                    }

                    // Now get an actor for this actorId and let it handle the request
                    TResponse? replyObj = default;
                    try
                    {
                        replyObj = await HandleActorRequest(actorId, requestObj);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"[ActorConsumer] Error in actor's HandleRequestAsync: {e}");
                    }

                    // Produce a reply only if there's a correlationId and a non-null reply
                    if (!string.IsNullOrEmpty(correlationId) && replyObj != null)
                    {
                        var replyJson = JsonSerializer.Serialize(replyObj);
                        var replyMessage = new Message<string, string>
                        {
                            // Optionally use actorId or something else as the key for replies
                            Key = actorId,
                            Value = replyJson,
                            Headers = new Headers
                            {
                                { "correlationId", Encoding.UTF8.GetBytes(correlationId) }
                            }
                        };

                        Console.WriteLine($"[ActorConsumer] Producing TResponse. CorrId={correlationId}");
                        await _producer.ProduceAsync(ReplyTopic, replyMessage);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ActorConsumer] Error: {ex.Message}");
            }
            finally
            {
                _actorConsumer.Close();
            }
        }, cancellationToken);

        // 2) Reply consumer: receives TResponse messages (serialized as JSON),
        //    matches correlationId to an in-flight request, completes TCS.
        var replyTask = Task.Run(() =>
        {
            Console.WriteLine($"Subscribing to {ReplyTopic}");
            _replyConsumer.Subscribe(ReplyTopic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = _replyConsumer.Consume(cancellationToken);
                    if (consumeResult == null)
                        continue;

                    var rawReplyJson = consumeResult.Message.Value;

                    // Extract correlationId
                    var correlationHeader = consumeResult.Message.Headers
                        .FirstOrDefault(h => h.Key == "correlationId");
                    var correlationId = correlationHeader != null
                        ? Encoding.UTF8.GetString(correlationHeader.GetValueBytes()!)
                        : null;

                    if (string.IsNullOrEmpty(correlationId))
                    {
                        // No correlation -> unsolicited
                        continue;
                    }

                    Console.WriteLine($"[ReplyConsumer] Received TResponse for correlationId={correlationId}");

                    // Try to find matching pending request
                    if (_pendingReplies.TryGetValue(correlationId, out var tcs))
                    {
                        try
                        {
                            var replyObj = JsonSerializer.Deserialize<TResponse>(rawReplyJson);
                            tcs.SetResult(replyObj);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ReplyConsumer] Deserialization error. correlationId={correlationId}, Ex={ex}");
                            tcs.SetException(ex);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ReplyConsumer] Error: {ex.Message}");
            }
            finally
            {
                _replyConsumer.Close();
            }
        }, cancellationToken);

        await Task.WhenAll(actorTask, replyTask);
    }

    /// <summary>
    /// Send a typed TRequest to actor_topic, wait for a typed TResponse.
    /// Returns null if timed out (or if deserialization yields null).
    /// </summary>
    public async Task<TResponse?> SendMessageAwaitReply(
        string actorId,
        TRequest requestObj,
        TimeSpan? timeout = null)
    {
        var correlationId = Guid.NewGuid().ToString();

        // The TCS we will await for the reply (whether local or remote).
        var tcs = new TaskCompletionSource<TResponse?>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // Register an in-flight request correlation
        _pendingReplies[correlationId] = tcs;

        try
        {
            // 1) Determine which partition this request belongs to
            var targetPartition = GetPartition(actorId);

            // 2) Check local consumer's assignment: do we own that partition?
            var assignedPartitions = _actorConsumer.Assignment;
            bool isLocal = assignedPartitions.Any(tp =>
                tp.Partition.Value == targetPartition && tp.Topic == ActorTopic);

            if (isLocal)
            {
                // 3) We own the partition => handle request locally, skip Kafka
                Console.WriteLine($"[Request] Local partition={targetPartition} for actorId={actorId} => handle in-process.");
                try
                {
                    // Directly call our actor logic
                    var replyObj = await HandleActorRequest(actorId, requestObj);

                    // Complete the TCS with the local reply
                    tcs.SetResult(replyObj);
                }
                catch (Exception ex)
                {
                    // If something goes wrong, complete TCS with exception
                    tcs.SetException(ex);
                }
            }
            else
            {
                // 4) Not local => produce to Kafka as normal
                var rawRequestJson = JsonSerializer.Serialize(requestObj);
                var message = new Message<string, string>
                {
                    Key = actorId,
                    Value = rawRequestJson,
                    Headers = new Headers
                    {
                        { "correlationId", Encoding.UTF8.GetBytes(correlationId) }
                    }
                };

                Console.WriteLine($"[Request] actorId={actorId}, correlationId={correlationId}, partition={targetPartition} => producing to Kafka.");
                await _producer.ProduceAsync(ActorTopic, message);
            }

            // 5) Wait up to `timeout` for the TCS to be completed (by local or remote path)
            var actualTimeout = timeout ?? TimeSpan.FromSeconds(10);
            var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(actualTimeout));
            if (completedTask == tcs.Task)
            {
                // TCS was completed (locally or by reply consumer)
                return tcs.Task.Result;
            }
            else
            {
                // Timed out
                Console.WriteLine($"[Request] Timed out waiting for TResponse. correlationId={correlationId}");
                return default;
            }
        }
        finally
        {
            // Remove from in-flight dictionary
            _pendingReplies.TryRemove(correlationId, out _);
        }
    }

    /// <summary>
    /// Obtain an actor for the given actorId via _actorFactory, then let it handle the request.
    /// </summary>
    private async Task<TResponse?> HandleActorRequest(string actorId, TRequest requestObj)
    {
        var actor = _actorFactory(actorId); // create or retrieve the actor instance
        return await actor.HandleRequestAsync(requestObj);
    }

    /// <summary>
    /// Ensure required topics exist (actor_topic, actor_reply_...).
    /// </summary>
    private async Task EnsureTopicsExist()
    {
        using var adminClient = new AdminClientBuilder(_kafkaConfig).Build();

        // 1) Actor topic
        try
        {
            var mainTopicSpec = new TopicSpecification
            {
                Name = ActorTopic,
                NumPartitions = PartitionCount
            };
            await adminClient.CreateTopicsAsync(new List<TopicSpecification> { mainTopicSpec });
            Console.WriteLine($"Topic '{ActorTopic}' created successfully.");
        }
        catch (CreateTopicsException e) when (e.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // exists
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine($"Failed to create topic '{ActorTopic}': {e.Results[0].Error.Reason}");
        }

        // 2) Reply topic
        try
        {
            var replyTopicSpec = new TopicSpecification
            {
                Name = ReplyTopic,
                NumPartitions = 1
            };
            await adminClient.CreateTopicsAsync(new List<TopicSpecification> { replyTopicSpec });
            Console.WriteLine($"Topic '{ReplyTopic}' created successfully.");
        }
        catch (CreateTopicsException e) when (e.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // exists
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine($"Failed to create topic '{ReplyTopic}': {e.Results[0].Error.Reason}");
        }
    }

    /// <summary>
    /// Simple Murmur2-based hashing to figure out partitions for actorId.
    /// (Used in logs, or you could do a manual partition assignment if you like.)
    /// </summary>
    private int GetPartition(string key)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(key);
        int hash = KafkaHash.Murmur2(bytes) & 0x7fffffff;
        return hash % PartitionCount;
    }

    /// <summary>
    /// Loads or creates a stable ID for this client instance.
    /// </summary>
    private static string GetOrCreateClientId(string filePath)
    {
        if (File.Exists(filePath))
        {
            return File.ReadAllText(filePath).Trim();
        }

        var newId = Guid.NewGuid().ToString();
        File.WriteAllText(filePath, newId);
        return newId;
    }
}