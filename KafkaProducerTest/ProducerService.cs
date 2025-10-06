using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducerTest
{
    public class ProducerService : IDisposable
    {
        private IProducer<Null, string> Producer { get; init; }
        private string Topic { get; init; }

        public ProducerService()
        {
            var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
            Topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test-topic";

            var cfg = new ProducerConfig
            {
                BootstrapServers = bootstrap,
                Acks = Acks.All
            };

            Producer = new ProducerBuilder<Null, string>(cfg).Build();
        }

        /// <summary>Отправить сообщение</summary>
        /// <param name="message">Сообщение</param>
        /// <returns></returns>
        public async Task<DeliveryResult<Null, string>?> SendAsync(string message)
        {
            if (string.IsNullOrWhiteSpace(message)) return null;
            try
            {
                var dr = await Producer.ProduceAsync(Topic, new Message<Null, string> { Value = message });
                return dr;
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"Produce error: {ex.Error.Reason}");
                throw;
            }
        }

        public void Dispose()
        {
            try
            {
                Producer.Flush(TimeSpan.FromSeconds(5));
                Producer.Dispose();
            }
            catch { }
        }
    }
}
