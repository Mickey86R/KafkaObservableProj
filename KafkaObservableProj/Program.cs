using KafkaObservableProj.Data;
using KafkaObservableProj.DTO;
using KafkaObservableProj.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();

builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.AddSingleton<IDataStorage>(sp => DataStorageFactory.Create(sp.GetRequiredService<ILoggerFactory>()));
builder.Services.AddSingleton<IEventObservable, EventObservable>();
builder.Services.AddSingleton<IEventObserver, EventObserver>();

builder.Services.AddHostedService<SubscriptionHostedService>(); // subscribe observer to observable
builder.Services.AddHostedService<KafkaConsumer>(); // kafka consumer background service

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