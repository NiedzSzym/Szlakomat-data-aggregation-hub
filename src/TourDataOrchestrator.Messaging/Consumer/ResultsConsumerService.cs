using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Text.Json;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;
using TourDataOrchestrator.Messaging.Configuration;

namespace TourDataOrchestrator.Messaging.Consumer;

/// <summary>
/// BackgroundService nasłuchujący na kolejce zwrotnej (gather leg wzorca Scatter-Gather).
///
/// Strategia odporności: pętla retry w ExecuteAsync izoluje awarię połączenia od hosta.
/// Wyjątek który wydostałby się poza ExecuteAsync zatrzymuje cały proces
/// (BackgroundServiceExceptionBehavior.StopHost — domyślne w .NET).
/// </summary>
public sealed class ResultsConsumerService : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    private readonly RabbitMqOptions _options;
    private readonly IScrapingResultAggregator _aggregator;
    private readonly ILogger<ResultsConsumerService> _logger;

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
        // Zewnętrzna pętla retry: serwis przeżywa chwilowy brak brokera
        // zarówno przy starcie, jak i w trakcie działania aplikacji.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAndConsumeAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Normalne zatrzymanie przez stoppingToken — wychodzimy bez błędu.
                break;
            }
            catch (BrokerUnreachableException ex)
            {
                _logger.LogWarning(
                    "RabbitMQ niedostępny: {Message}. Ponowna próba za {Delay}s…",
                    ex.Message, RetryDelay.TotalSeconds);

                await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            catch (AlreadyClosedException ex)
            {
                _logger.LogWarning(
                    "Połączenie z RabbitMQ zostało zamknięte: {Message}. Ponowna próba za {Delay}s…",
                    ex.Message, RetryDelay.TotalSeconds);

                await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Nieoczekiwany błąd konsumenta. Ponowna próba za {Delay}s…", RetryDelay.TotalSeconds);
                await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }
    }

    private async Task ConnectAndConsumeAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.Host,
            Port = _options.Port,
            UserName = _options.Username,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
        };

        await using var connection = await factory.CreateConnectionAsync(
            clientProvidedName: "orchestrator-consumer",
            cancellationToken: stoppingToken);

        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: _options.ResultsQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // prefetchCount=10: back-pressure — konsument nie pobierze więcej wiadomości
        // niż jest w stanie przetworzyć, zanim wyśle ACK.
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, ea) => OnMessageReceivedAsync(channel, ea);

        await channel.BasicConsumeAsync(
            queue: _options.ResultsQueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Konsument wyników uruchomiony na kolejce '{Queue}'", _options.ResultsQueueName);

        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private async Task OnMessageReceivedAsync(IChannel channel, BasicDeliverEventArgs ea)
    {
        WorkerResultMessage? result = null;
        try
        {
            result = JsonSerializer.Deserialize<WorkerResultMessage>(ea.Body.Span);

            if (result is null)
            {
                _logger.LogWarning("Odebrano pustą lub nieprawidłową wiadomość. Tag: {Tag}", ea.DeliveryTag);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            await _aggregator.AggregateAsync(result.TaskId, result);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

            _logger.LogDebug(
                "Zagregowano wynik od '{Target}' dla zadania {TaskId}. Sukces: {Success}",
                result.Target, result.TaskId, result.Success);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Błąd deserializacji wiadomości. Tag: {Tag}", ea.DeliveryTag);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd agregacji wyniku dla zadania {TaskId}", result?.TaskId);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
        }
    }
}
