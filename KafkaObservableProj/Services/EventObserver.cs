using KafkaObservableProj.Data;
using KafkaObservableProj.DTO;
using KafkaObservableProj.Models;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Reflection;

namespace KafkaObservableProj.Services
{
    public interface IEventObserver : IObserver<UserEvent>
    {
        /// <summary>Получение статистики по полученным сообщениям</summary>
        /// <returns></returns>
        public List<UserEventStat> GetSnapshot();

        /// <summary>Получение статистики с фильтром по типу события</summary>
        /// <param name="typeFilter">фильтр (click и т.п.)</param>
        /// <returns></returns>
        public List<UserEventStat> GetSnapshot(string typeFilter);

        /// <summary>Получение всех сообщений, полученных в указанном временном диапазоне</summary>
        /// <param name="from">Время от</param>
        /// <param name="to">Время до</param>
        /// <returns></returns>
        public List<UserEvent> GetSnapshot(DateTime? from, DateTime? to);
    }

    public class EventObserver : IEventObserver, IDisposable
    {
        private IDataStorage Storage { get; init; }
        private ILogger<EventObserver> Logger { get; init; }
        private ConcurrentDictionary<(int userId, string eventType), long> Counters { get; set; } = [];
        private List<UserEvent> Messages { get; set; } = [];
        private CancellationTokenSource Cts { get; set; } = new();
        private Task FlushTask { get; init; }
        private int FlushSeconds { get; init; }
        private string? FilterEventType { get; init; }

        public EventObserver(IDataStorage storage, ILogger<EventObserver> logger)
        {
            Storage = storage ?? throw new ArgumentNullException(nameof(storage));
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));

            FlushSeconds = int.TryParse(Environment.GetEnvironmentVariable("FLUSH_INTERVAL_SECONDS", EnvironmentVariableTarget.User), out var fs) && fs > 0
                            ? fs
                            : 10;

            FilterEventType = Environment.GetEnvironmentVariable("EVENT_FILTER_TYPE", EnvironmentVariableTarget.User);

            FlushTask = Task.Run(() => FlushLoopAsync(Cts.Token));
        }

        public void OnNext(UserEvent value)
        {
            if (value == null) return;
            if (!string.IsNullOrEmpty(FilterEventType) &&
                !string.Equals(value.EventType, FilterEventType, StringComparison.OrdinalIgnoreCase))
                return;

            var key = (value.UserId, value.EventType ?? string.Empty);

            Messages.Add(value);
            Counters.AddOrUpdate(key, 1, (_, existing) => existing + 1);
        }

        public void OnError(Exception error) => Logger.LogError(error, "Observable error");

        public void OnCompleted()
        {
            Logger.LogInformation("Observable completed. Flushing.");
            FlushAsync().GetAwaiter().GetResult();
        }

        private async Task FlushLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(FlushSeconds), ct);
                    await FlushAsync();
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Logger.LogError(ex, "Flush loop error"); }
        }

        // Публичный метод, который можно вызвать извне (например, из контроллера)
        public async Task FlushAsync()
        {
            var snapshot = DrainSnapshot();

            if (snapshot.Count == 0) return;

            try
            {
                Logger.LogInformation("Flushing {Count} stats", snapshot.Count);
                await Storage.SaveAsync(snapshot);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to save stats");
            }
        }

        /// <summary>Возврат текущей статистики</summary>
        /// <returns></returns>
        public List<UserEventStat> GetSnapshot()
        {
            var list = new List<UserEventStat>();
            foreach (var kv in Counters)
            {
                list.Add(new UserEventStat
                {
                    UserId = kv.Key.userId,
                    EventType = kv.Key.eventType,
                    Count = kv.Value
                });
            }
            return list;
        }

        /// <summary>Возврат текущей статистики по фильтру типа события</summary>
        /// <param name="typeFilter">Фильтр типа события</param>
        /// <returns></returns>
        public List<UserEventStat> GetSnapshot(string typeFilter)
        {
            var result = Messages.Where(m => m.EventType == typeFilter)
                                    .GroupBy(m => m.UserId)
                                    .Select(g => new UserEventStat { UserId = g.Key, EventType = typeFilter, Count = g.Count() })
                                    .ToList();

            return result;
        }

        /// <summary>Возврат коллекции сообщений, полученных во временном диапазоне</summary>
        /// <param name="from">Начальное время</param>
        /// <param name="to">Конечное время</param>
        /// <returns></returns>
        public List<UserEvent> GetSnapshot(DateTime? from, DateTime? to)
        {
            var result = Messages.Where(m => m.Timestamp >= (from ?? DateTime.MinValue) && m.Timestamp<= (to ?? DateTime.UtcNow))
                                    .ToList();

            return result;
        }

        
        private List<UserEventStat> DrainSnapshot()
        {
            var snapshot = new List<UserEventStat>();
            foreach (var kv in Counters)
            {
                if (Counters.TryRemove(kv.Key, out var value))
                {
                    snapshot.Add(new UserEventStat
                    {
                        UserId = kv.Key.userId,
                        EventType = kv.Key.eventType,
                        Count = value
                    });
                }
                else
                {
                    if (Counters.TryGetValue(kv.Key, out var cur))
                    {
                        if (Counters.TryUpdate(kv.Key, 0, cur))
                        {
                            snapshot.Add(new UserEventStat
                            {
                                UserId = kv.Key.userId,
                                EventType = kv.Key.eventType,
                                Count = cur
                            });
                        }
                    }
                }
            }

            return snapshot;
        }

        public void Dispose()
        {
            Cts.Cancel();

            try { FlushTask.Wait(TimeSpan.FromSeconds(5)); } catch { }

            FlushAsync().GetAwaiter().GetResult();
            Cts.Dispose();
        }
    }
}
