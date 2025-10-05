using KafkaObservableProj.Data;
using KafkaObservableProj.DTO;
using System.Collections.Concurrent;

namespace KafkaObservableProj.Services
{
    public class UserEventStat
    {
        public int UserId { get; init; }
        public string EventType { get; init; } = string.Empty;
        public long Count { get; set; }
    }

    // Простой observer: считает события и периодически флашит в IDataStorage
    public class EventObserver : IObserver<UserEvent>, IDisposable
    {
        private readonly IDataStorage _storage;
        private readonly ILogger<EventObserver> _logger;
        private readonly ConcurrentDictionary<(int userId, string eventType), long> _counters = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _flushTask;
        private readonly int _flushSeconds;
        private readonly string? _filterEventType;

        public EventObserver(IDataStorage storage, ILogger<EventObserver> logger, int flushSeconds = 10, string? filterEventType = null)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _flushSeconds = Math.Max(1, flushSeconds);
            _filterEventType = filterEventType;
            _flushTask = Task.Run(() => FlushLoopAsync(_cts.Token));
        }

        public void OnNext(UserEvent value)
        {
            if (value == null) return;
            if (!string.IsNullOrEmpty(_filterEventType) &&
                !string.Equals(value.EventType, _filterEventType, StringComparison.OrdinalIgnoreCase))
                return;

            var key = (value.UserId, value.EventType ?? string.Empty);
            _counters.AddOrUpdate(key, 1, (_, existing) => existing + 1);
        }

        public void OnError(Exception error)
        {
            _logger.LogError(error, "Observable error");
        }

        public void OnCompleted()
        {
            _logger.LogInformation("Observable completed. Flushing.");
            // синхронно флашим
            FlushAsync().GetAwaiter().GetResult();
        }

        private async Task FlushLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(_flushSeconds), ct);
                    await FlushAsync();
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Flush loop error");
            }
        }

        public async Task FlushAsync()
        {
            var snapshot = new List<UserEventStat>();

            foreach (var kv in _counters)
            {
                // Попытка удалить ключ и взять значение — best-effort
                if (_counters.TryRemove(kv.Key, out var value))
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
                    // Если не удалось удалить (гонка), пробуем получить текущее значение и сбросить на 0
                    if (_counters.TryGetValue(kv.Key, out var cur))
                    {
                        if (_counters.TryUpdate(kv.Key, 0, cur))
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

            if (snapshot.Count == 0) return;

            try
            {
                _logger.LogInformation("Flushing {Count} stats", snapshot.Count);
                await _storage.SaveAsync(snapshot);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to save stats");
                // При ошибке можно: повторять, сохранять локально и т.д. — не реализовано (упрощённо)
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            try { _flushTask.Wait(TimeSpan.FromSeconds(5)); } catch { }
            FlushAsync().GetAwaiter().GetResult();
            _cts.Dispose();
        }
    }
}
