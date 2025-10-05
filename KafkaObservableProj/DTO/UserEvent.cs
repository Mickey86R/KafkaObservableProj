namespace KafkaObservableProj.DTO
{
    public class UserEvent
    {
        public int UserId { get; init; }
        public string EventType { get; init; } = string.Empty;
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
        public UserEventData Data { get; init; }
    }

    public class UserEventData
    {
        public string ButtonId { get; init; }
    }
}
