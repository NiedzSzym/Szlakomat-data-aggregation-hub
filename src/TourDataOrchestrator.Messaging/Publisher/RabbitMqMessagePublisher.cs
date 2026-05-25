using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text.Json;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Messaging.Configuration;

namespace TourDataOrchestrator.Messaging.Publisher;

/// <summary>
/// Implementacja <see cref="IMessagePublisher"/> oparta na RabbitMQ.Client 7.x (async-first API).
///
/// Strategia zarządzania zasobami:
/// - <see cref="IConnection"/> — singleton, współdzielony przez wszystkie wywołania (koszt TCP).
/// - <see cref="IChannel"/> — tworzony i usuwany per-publish; kanały są lekkie, ale nie thread-safe.
/// - <see cref="_connectionLock"/> (SemaphoreSlim) realizuje wzorzec Double-Checked Locking dla
///   leniwej inicjalizacji połączenia bez ryzyka Race Condition przy starcie pod obciążeniem.
/// </summary>
public sealed class RabbitMqMessagePublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqMessagePublisher> _logger;

    private IConnection? _connection;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    public RabbitMqMessagePublisher(
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqMessagePublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc/>
    public async Task PublishAsync<T>(T message, string routingKey, CancellationToken cancellationToken = default)
        where T : class
    {
        var connection = await GetOrCreateConnectionAsync(cancellationToken);

        // IChannel jest IAsyncDisposable; używamy osobnego kanału per-call, aby uniknąć
        // współdzielonego stanu między równoległymi publikacjami (kanał nie jest thread-safe).
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        // Idempotentna deklaracja giełdy — bezpieczne do wywołania przy każdej publikacji.
        // Typ Topic pozwala workerom subskrybować wzorce kluczy, np. "task.scrap.*".
        await channel.ExchangeDeclareAsync(
            exchange: _options.TaskExchangeName,
            type: ExchangeType.Topic,
            durable: true,     // przeżywa restart brokera
            autoDelete: false,
            cancellationToken: cancellationToken);

        var body = JsonSerializer.SerializeToUtf8Bytes(message);

        var properties = new BasicProperties
        {
            Persistent = true,              // MessageDeliveryMode.Persistent: wiadomość przeżyje restart brokera
            ContentType = "application/json",
            ReplyTo = _options.ResultsQueueName,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };

        await channel.BasicPublishAsync(
            exchange: _options.TaskExchangeName,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);

        _logger.LogDebug(
            "Opublikowano wiadomość {MessageType} na giełdę '{Exchange}' z kluczem routingu '{RoutingKey}'",
            typeof(T).Name, _options.TaskExchangeName, routingKey);
    }

    /// <summary>
    /// Leniwa inicjalizacja połączenia z Double-Checked Locking.
    /// AutomaticRecoveryEnabled jest domyślnie aktywne w kliencie 7.x —
    /// broker może chwilowo spaść bez utraty połączenia z punktu widzenia producenta.
    /// </summary>
    private async Task<IConnection> GetOrCreateConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
            return _connection;

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true })
                return _connection;

            var factory = new ConnectionFactory
            {
                HostName = _options.Host,
                Port = _options.Port,
                UserName = _options.Username,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                // AutomaticRecoveryEnabled = true (domyślnie) — klient samodzielnie odbudowuje
                // połączenie TCP po awarii brokera bez interwencji aplikacji.
            };

            _connection = await factory.CreateConnectionAsync(
                clientProvidedName: "orchestrator-producer",
                cancellationToken: cancellationToken);

            _logger.LogInformation(
                "Nawiązano połączenie z RabbitMQ: {Host}:{Port}/{VHost}",
                _options.Host, _options.Port, _options.VirtualHost);

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
            await _connection.DisposeAsync();

        _connectionLock.Dispose();
    }
}
