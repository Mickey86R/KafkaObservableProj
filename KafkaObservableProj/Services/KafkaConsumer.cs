using Confluent.Kafka;
using KafkaObservableProj.DTO;
using System.Text.Json;

namespace KafkaObservableProj.Services
{
    public class KafkaConsumer : BackgroundService
    {
        private IEventObservable Observable { get; init; }
        private ILogger<KafkaConsumer> Logger { get; init; }

        private string Bootstrap { get; init; }
        private string Topic { get; init; }
        private string GroupId { get; init; }

        public KafkaConsumer(IEventObservable observable, ILogger<KafkaConsumer> logger)
        {
            Observable = observable;
            Logger = logger;
            Bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
            Topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test-topic";
            GroupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "user-events-group";
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
        }

        private void ConsumeLoop(CancellationToken stoppingToken)
        {
            var conf = new ConsumerConfig
            {
                BootstrapServers = Bootstrap,
                GroupId = GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                //EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
            consumer.Subscribe(Topic);
            Logger.LogInformation($"Subscribed to {Topic}", Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                        if (cr?.Message?.Value == null) continue;

                        var payload = cr.Message.Value;

                        UserEvent? ev = null;
                        try
                        {
                            ev = JsonSerializer.Deserialize<UserEvent>(payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                        }
                        catch (JsonException je)
                        {
                            Logger.LogWarning(je, "Invalid JSON: {Payload}", payload);
                        }

                        if (ev != null)
                            Observable.Publish(ev);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                    catch (ConsumeException cex) { Logger.LogError(cex, "Consume error"); }
                    catch (Exception ex) { Logger.LogError(ex, "Unexpected error"); }
                }
            }
            finally
            {
                try { consumer.Close(); } catch { }
                Observable.PublishCompleted();
                Logger.LogInformation("Kafka consumer stopped");
            }
        }
    }
}
