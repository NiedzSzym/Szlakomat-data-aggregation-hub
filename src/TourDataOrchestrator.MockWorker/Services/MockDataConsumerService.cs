using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Text.Json;
using TourDataOrchestrator.Application.DTOs;
using TourDataOrchestrator.MockWorker.Configuration;

namespace TourDataOrchestrator.MockWorker.Services;

/// <summary>
/// Mock implementacja workera danych. Demonstruje wzorzec podpinania providerów:
/// każdy worker deklaruje własną kolejkę i binduje ją do wspólnego Exchange
/// z Routing Key specyficznym dla obsługiwanego zasobu.
/// </summary>
public sealed class MockDataConsumerService : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    private readonly MockWorkerOptions _options;
    private readonly ILogger<MockDataConsumerService> _logger;

    public MockDataConsumerService(
        IOptions<MockWorkerOptions> options,
        ILogger<MockDataConsumerService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAndConsumeAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
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
                    "Połączenie zamknięte: {Message}. Ponowna próba za {Delay}s…",
                    ex.Message, RetryDelay.TotalSeconds);

                await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Nieoczekiwany błąd workera. Ponowna próba za {Delay}s…", RetryDelay.TotalSeconds);
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
            clientProvidedName: $"mock-worker-{_options.QueueName}",
            cancellationToken: stoppingToken);

        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.ExchangeDeclareAsync(
            exchange: _options.TaskExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(
            queue: _options.QueueName,
            exchange: _options.TaskExchangeName,
            routingKey: _options.BindingKey,
            cancellationToken: stoppingToken);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 5, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, ea) => OnTaskReceivedAsync(channel, ea);

        await channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation(
            "MockWorker gotowy. Kolejka: '{Queue}', Binding: '{Key}' → Exchange: '{Exchange}'",
            _options.QueueName, _options.BindingKey, _options.TaskExchangeName);

        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private async Task OnTaskReceivedAsync(IChannel channel, BasicDeliverEventArgs ea)
    {
        WorkerTaskMessage? task = null;
        try
        {
            task = JsonSerializer.Deserialize<WorkerTaskMessage>(ea.Body.Span);

            if (task is null)
            {
                _logger.LogWarning("Odebrano pustą wiadomość. Tag: {Tag}", ea.DeliveryTag);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            _logger.LogInformation(
                "Otrzymano zadanie {TaskId} | Target: {Target} | Intent: {Intent}",
                task.TaskId, task.Target, task.Intent);

            var replyTo = ea.BasicProperties.ReplyTo ?? task.ReplyToQueue;

            var result = new WorkerResultMessage(
                TaskId: task.TaskId,
                Target: task.Target,
                Success: true,
                Payload: BuildMockPayload(task),
                Error: null);

            var body = JsonSerializer.SerializeToUtf8Bytes(result);
            var replyProps = new BasicProperties
            {
                ContentType = "application/json",
                CorrelationId = task.TaskId,
            };

            await channel.BasicPublishAsync(
                exchange: "",
                routingKey: replyTo,
                mandatory: false,
                basicProperties: replyProps,
                body: body);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

            _logger.LogInformation(
                "Wysłano odpowiedź dla zadania {TaskId} → '{ReplyTo}'",
                task.TaskId, replyTo);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Błąd deserializacji zadania. Tag: {Tag}", ea.DeliveryTag);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd przetwarzania zadania {TaskId}", task?.TaskId);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
        }
    }

    private static object BuildMockPayload(WorkerTaskMessage task) => new
    {
        source = "mock_worker_dotnet",
        target = task.Target,
        intent = task.Intent,
        name = $"Mock: {task.Target}",
        price_adult_pln = 35,
        price_child_pln = 20,
        available = true,
        available_slots = new[] { "09:00", "11:00", "13:00", "15:00" },
        description = "Statyczna odpowiedź .NET mock workera.",
        generated_at_utc = DateTime.UtcNow,
    };
}
