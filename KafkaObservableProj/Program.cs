using KafkaObservableProj.Data;
using KafkaObservableProj.DTO;
using KafkaObservableProj.Services;

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(l =>
    {
        l.ClearProviders();
        l.AddConsole();
        l.SetMinimumLevel(LogLevel.Information);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddSingleton<EventObservable>();
        services.AddSingleton<IObservable<UserEvent>>(sp => sp.GetRequiredService<EventObservable>());

        services.AddSingleton<IDataStorage>(sp => DataStorageFactory.Create(sp.GetRequiredService<ILoggerFactory>()));

        var flushSeconds = 10;
        if (int.TryParse(Environment.GetEnvironmentVariable("FLUSH_INTERVAL_SECONDS"), out var f) && f > 0)
            flushSeconds = f;
        var filter = Environment.GetEnvironmentVariable("EVENT_FILTER_TYPE");

        services.AddSingleton<EventObserver>(sp =>
            new EventObserver(sp.GetRequiredService<IDataStorage>(), sp.GetRequiredService<ILogger<EventObserver>>(), flushSeconds, filter));

        services.AddSingleton<IObserver<UserEvent>>(sp => sp.GetRequiredService<EventObserver>());

        services.AddHostedService<SubscriptionHostedService>();
        services.AddHostedService<KafkaConsumer>();
    })
    .Build();

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();


builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();


internal class SubscriptionHostedService : IHostedService
{
    private readonly EventObservable _observable;
    private readonly IObserver<UserEvent> _observer;
    private IDisposable? _sub;
    private readonly ILogger<SubscriptionHostedService> _logger;

    public SubscriptionHostedService(EventObservable observable, IObserver<UserEvent> observer, ILogger<SubscriptionHostedService> logger)
    {
        _observable = observable;
        _observer = observer;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Subscribing observer");
        _sub = _observable.Subscribe(_observer);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Unsubscribing observer");
        _sub?.Dispose();
        return Task.CompletedTask;
    }
}