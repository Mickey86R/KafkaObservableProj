namespace KafkaObservableProj.Models
{
    public class UserEventStat
    {
        public int UserId { get; set; }
        public string EventType { get; set; } = string.Empty;
        public long Count { get; set; }
    }
}
