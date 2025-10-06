using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KafkaProducerTest.Models
{
    public class UserEvent
    {
        [JsonPropertyName("userId")]
        public int UserId { get; set; }

        [JsonPropertyName("eventType")]
        public string EventType { get; set; } = string.Empty;

        [JsonPropertyName("timestamp")]
        public DateTimeOffset Timestamp { get; set; }

        [JsonPropertyName("data")]
        public EventData Data { get; set; } = new EventData();
    }

    public class EventData
    {
        [JsonPropertyName("buttonId")]
        public string ButtonId { get; set; } = string.Empty;
    }
}
