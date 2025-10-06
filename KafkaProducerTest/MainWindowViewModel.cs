using Confluent.Kafka;
using KafkaProducerTest.Helpers;
using KafkaProducerTest.Models;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;

namespace KafkaProducerTest
{

    public enum SendMode { Single, Series }

    public class MainWindowViewModel : INotifyPropertyChanged
    {
        /// <summary>KafkaProducer</summary>
        private ProducerService Producer { get; init; }

        /// <summary>Рандом для генерации ID пользователя</summary>
        private Random Rnd { get; init; } = new();

        /// <summary>Типы событий</summary>
        private string[] EventTypes { get; } = new[] { "click", "doubleclick", "tooltip" };

        /// <summary>ID "Кнопок"</summary>
        private string[] ButtonIds { get; } = new[] { "submit", "confirm", "roll", "open", "close" };

        public string KafkaBootstrapServers { get; } = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
        public string KafkaTopic { get; } = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test-topic";
        public string KafkaGroupId { get; } = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "kafka-producer-test";


        private int _count = 10;
        /// <summary>Количество отправляемых сообщений</summary>
        public int Count { get => _count; set => Set(ref _count, value); }


        private int _intervalMs = 500;
        /// <summary>Интервал при отправке серии сообщений</summary>
        public int IntervalMs { get => _intervalMs; set => Set(ref _intervalMs, value); }

        private string _selectedMessageJson = string.Empty;
        /// <summary>Пользовательское сообщение</summary>
        public string SelectedMessageJson { get => _selectedMessageJson; set => Set(ref _selectedMessageJson, value); }

        /// <summary>отправленные сообщения</summary>
        public ObservableCollection<string> SentMessages { get; } = new();

        /// <summary>Отправить одно сообщение</summary>
        public RelayCommand SendOneCommand { get; }

        /// <summary>Старт отправки серии сообщений</summary>
        public RelayCommand StartSeriesCommand { get; }

        /// <summary>Прекратить отправку серии сообщений</summary>
        public RelayCommand StopSeriesCommand { get; }

        /// <summary>Сгенерировать сообщение</summary>
        public RelayCommand GenerateSampleCommand { get; }

        private CancellationTokenSource? Cts { get; set; }

        private bool _isRunning = false;
        /// <summary>Команда выполняется</summary>
        public bool IsRunning { get => _isRunning; private set { Set(ref _isRunning, value); UpdateCommands(); } }

        public MainWindowViewModel()
        {
            Producer = new ProducerService();
            SendOneCommand = new RelayCommand(async _ => await SendOneAsync(), _ => !IsRunning);
            StartSeriesCommand = new RelayCommand(async _ => await StartSeriesAsync(), _ => !IsRunning);
            StopSeriesCommand = new RelayCommand(_ => StopSeries(), _ => IsRunning);
            GenerateSampleCommand = new RelayCommand(_ => GenerateSampleMessage());

            GenerateSampleMessage();
        }

        private void UpdateCommands()
        {
            SendOneCommand.RaiseCanExecuteChanged();
            StartSeriesCommand.RaiseCanExecuteChanged();
            StopSeriesCommand.RaiseCanExecuteChanged();
        }

        /// <summary>Генерировать сообщение на отправку</summary>
        private void GenerateSampleMessage()
        {
            var ev = GenerateRandomEvent();
            SelectedMessageJson = JsonSerializer.Serialize(ev, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, WriteIndented = true });
        }

        /// <summary>Генерировать экземпляр UserEvent</summary>
        /// <returns>Экземпляр UserEvent</returns>
        private UserEvent GenerateRandomEvent()
        {
            return new UserEvent
            {
                UserId = Rnd.Next(1, 1000000),
                EventType = EventTypes[Rnd.Next(EventTypes.Length)],
                Timestamp = DateTimeOffset.UtcNow,
                Data = new EventData { ButtonId = ButtonIds[Rnd.Next(ButtonIds.Length)] }
            };
        }

        /// <summary>Отправить одно сообщение</summary>
        /// <returns></returns>
        public async Task SendOneAsync()
        {
            var message = SelectedMessageJson;

            try
            {
                var dr = await Producer.SendAsync(message);
                var entry = $"{DateTime.Now:HH:mm:ss} => Sent (status {dr?.Status.ToString() ?? "?"}): {message}";
                SentMessages.Insert(0, entry);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to send: {ex.Message}", "Send Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>Запуск серии сообщений</summary>
        /// <returns></returns>
        public async Task StartSeriesAsync()
        {
            IsRunning = true;
            Cts = new CancellationTokenSource();
            var cancelToken = Cts.Token;

            int toSend = Count;
            bool infinite = Count == -1;
            int sent = 0;

            try
            {
                while (!cancelToken.IsCancellationRequested && (infinite || sent < toSend))
                {
                    var ev = GenerateRandomEvent();
                    var message = JsonSerializer.Serialize(ev, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, WriteIndented = false });

                    try
                    {
                        var dr = await Producer.SendAsync(message);

                        var entry = $"{DateTime.Now:HH:mm:ss} => Sent (status {dr?.Status.ToString() ?? "?"}): {message}";

                        App.Current.Dispatcher.Invoke(() => SentMessages.Insert(0, entry));
                    }
                    catch (Exception ex)
                    {
                        App.Current.Dispatcher.Invoke(() => SentMessages.Insert(0, $"{DateTime.Now:HH:mm:ss} => Error: {ex.Message}"));
                    }

                    sent++;

                    if (IntervalMs > 0)
                    {
                        try { await Task.Delay(IntervalMs, cancelToken); }
                        catch (OperationCanceledException) { break; }
                    }
                }
            }
            finally
            {
                StopSeries();
            }
        }

        /// <summary>Остановить отправку</summary>
        public void StopSeries()
        {
            if (Cts != null && !Cts.IsCancellationRequested)
            {
                Cts.Cancel();
            }

            IsRunning = false;
        }

        public void Dispose()
        {
            Producer?.Dispose();
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        protected void OnPropertyChanged([CallerMemberName] string? name = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        protected bool Set<T>(ref T field, T value, [CallerMemberName] string? propName = null)
        {
            if (Equals(field, value)) return false;

            field = value;
            OnPropertyChanged(propName);

            return true;
        }
    }
}
