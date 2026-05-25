using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;
using TourDataOrchestrator.Messaging.Configuration;

namespace TourDataOrchestrator.Messaging.Consumer;

/// <summary>
/// BackgroundService nasłuchujący na kolejce zwrotnej (gather leg wzorca Scatter-Gather).
/// Każda odebrana wiadomość to wynik jednego workera; po zebraniu wszystkich odpowiedzi
/// delegujemy agregację do <see cref="IScrapingResultAggregator"/>.
/// </summary>
public sealed class ResultsConsumerService : BackgroundService
{
    private readonly RabbitMqOptions _options;
    private readonly IScrapingResultAggregator _aggregator;
    private readonly ILogger<ResultsConsumerService> _logger;

    private IConnection? _connection;
    private IChannel? _channel;

    public ResultsConsumerService(
        IOptions<RabbitMqOptions> options,
        IScrapingResultAggregator aggregator,
        ILogger<ResultsConsumerService> logger)
    {
        _options = options.Value;
        _aggregator = aggregator;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.Host,
            Port = _options.Port,
            UserName = _options.Username,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
        };

        _connection = await factory.CreateConnectionAsync(
            clientProvidedName: "orchestrator-consumer",
            cancellationToken: stoppingToken);

        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

        // Deklaracja kolejki zwrotnej jako Durable — wiadomości nie przepadną przy restarcie brokera.
        await _channel.QueueDeclareAsync(
            queue: _options.ResultsQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // prefetchCount=10: ogranicza liczbę niepotwierdzonych wiadomości (back-pressure).
        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageReceivedAsync;

        await _channel.BasicConsumeAsync(
            queue: _options.ResultsQueueName,
            autoAck: false,     // manualne ACK po pomyślnej agregacji
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Konsument wyników uruchomiony na kolejce '{Queue}'", _options.ResultsQueueName);

        // Utrzymujemy serwis aktywny do momentu otrzymania sygnału zatrzymania.
        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs ea)
    {
        WorkerResultMessage? result = null;
        try
        {
            result = JsonSerializer.Deserialize<WorkerResultMessage>(ea.Body.Span);

            if (result is null)
            {
                _logger.LogWarning("Odebrano pustą lub nieprawidłową wiadomość. Tag dostawy: {Tag}", ea.DeliveryTag);
                await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            await _aggregator.AggregateAsync(result.TaskId, result);

            // ACK po pomyślnym przetworzeniu — gwarancja at-least-once delivery.
            await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false);

            _logger.LogDebug(
                "Zagregowano wynik od workera '{Target}' dla zadania {TaskId}. Sukces: {Success}",
                result.Target, result.TaskId, result.Success);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Błąd deserializacji wiadomości. Tag: {Tag}", ea.DeliveryTag);
            // Dead-letter: nie stawiamy ponownie w kolejce — uszkodzona wiadomość trafi do DLX.
            await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Nieoczekiwany błąd podczas agregacji wyniku dla zadania {TaskId}", result?.TaskId);
            await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        if (_channel is not null)
            await _channel.DisposeAsync();

        if (_connection is not null)
            await _connection.DisposeAsync();
    }
}
