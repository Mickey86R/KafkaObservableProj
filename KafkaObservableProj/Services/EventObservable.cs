using KafkaObservableProj.DTO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reactive.Disposables;

namespace KafkaObservableProj.Services
{
    public interface IEventObservable : IObservable<UserEvent>
    {
        public void Publish(UserEvent userEvent);

        public void PublishCompleted();
    }

    public class EventObservable : IEventObservable
    {
        private ConcurrentDictionary<IEventObserver, byte> Observers { get; init; } = new();

        public IDisposable Subscribe(IEventObserver observer)
        {
            if (observer == null) throw new ArgumentNullException(nameof(observer));
            Observers.TryAdd(observer, 0);

            return Disposable.Create(() =>
            {
                Observers.TryRemove(observer, out _);
            });
        }

        public IDisposable Subscribe(IObserver<UserEvent> observer)
        {
            if (observer == null) throw new ArgumentNullException(nameof(observer));
            Observers.TryAdd(observer as IEventObserver, 0);

            return Disposable.Create(() =>
            {
                Observers.TryRemove(observer as IEventObserver, out _);
            });
        }

        public void Publish(UserEvent ev)
        {
            foreach (var observer in Observers.Keys)
            {
                try { observer.OnNext(ev); } catch { }
            }
        }

        public void PublishError(Exception ex)
        {
            foreach (var observer in Observers.Keys)
            {
                try { observer.OnError(ex); } catch { }
            }
        }

        public void PublishCompleted()
        {
            foreach (var observer in Observers.Keys)
            {
                try { observer.OnCompleted(); } catch { }
            }
        }
    }
}
