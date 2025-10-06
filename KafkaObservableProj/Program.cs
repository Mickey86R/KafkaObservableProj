using KafkaObservableProj.Data;
using KafkaObservableProj.DTO;
using KafkaObservableProj.Services;

var builder = WebApplication.CreateBuilder(args);

// logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// controllers
builder.Services.AddControllers();

builder.Services.AddSingleton<IDataStorage>(sp => DataStorageFactory.Create(sp.GetRequiredService<ILoggerFactory>()));

// DI registrations (singletons so controller and hosted services share same instances)
builder.Services.AddSingleton<IEventObservable, EventObservable>();
//builder.Services.AddSingleton<IObservable<UserEvent>>(sp => sp.GetRequiredService<EventObservable>());

// DataStorage selection by env (POSTGRES_CONNECTION_STRING -> Postgres, else file)
//builder.Services.AddSingleton<IDataStorage>(sp => DataStorageFactory.Create(sp.GetRequiredService<ILoggerFactory>()));

// EventObserver registration (singleton)


//builder.Services.AddSingleton<EventObserver>(sp =>
//    new EventObserver(sp.GetRequiredService<IDataStorage>(),
//                      sp.GetRequiredService<ILogger<EventObserver>>()));
builder.Services.AddSingleton<IEventObserver, EventObserver>();

// Register as IObserver<UserEvent>
//builder.Services.AddSingleton<IObserver<UserEvent>, >(sp => sp.GetRequiredService<EventObserver>());

// Hosted services
builder.Services.AddHostedService<SubscriptionHostedService>(); // subscribe observer to observable
builder.Services.AddHostedService<KafkaConsumer>(); // kafka consumer background service

var app = builder.Build();

app.MapControllers();
app.MapGet("/health", () => Results.Ok("ok"));

app.Run();


// Hosted service to subscribe observer to observable at startup
internal class SubscriptionHostedService : IHostedService
{
    private readonly IEventObservable _observable;
    private readonly IEventObserver _observer;

    public SubscriptionHostedService(IEventObservable observable, IEventObserver observer)
    {
        _observable = observable;
        _observer = observer;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _observable.Subscribe(_observer);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}