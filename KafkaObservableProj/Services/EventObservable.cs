using KafkaObservableProj.DTO;
using System.Collections.Concurrent;
using System.Reactive.Disposables;

namespace KafkaObservableProj.Services
{
    // Упрощённая реализация IObservable<UserEvent>
    public class EventObservable : IObservable<UserEvent>
    {
        private readonly ConcurrentDictionary<IObserver<UserEvent>, byte> _observers = new();

        public IDisposable Subscribe(IObserver<UserEvent> observer)
        {
            if (observer == null) throw new ArgumentNullException(nameof(observer));
            _observers.TryAdd(observer, 0);

            // Возвращаем готовый Disposable из System.Reactive,
            // который при Dispose удалит наблюдателя из коллекции.
            return Disposable.Create(() =>
            {
                _observers.TryRemove(observer, out _);
            });
        }

        public void Publish(UserEvent ev)
        {
            foreach (var observer in _observers.Keys)
            {
                try { observer.OnNext(ev); } catch { /* подписчики обрабатывают ошибки сами */ }
            }
        }

        public void PublishError(Exception ex)
        {
            foreach (var observer in _observers.Keys)
            {
                try { observer.OnError(ex); } catch { }
            }
        }

        public void PublishCompleted()
        {
            foreach (var observer in _observers.Keys)
            {
                try { observer.OnCompleted(); } catch { }
            }
        }
    }

}
