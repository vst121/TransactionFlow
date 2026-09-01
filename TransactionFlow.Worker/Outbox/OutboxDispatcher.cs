using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Text.Json;
using TransactionFlow.Contracts.Events;
using TransactionFlow.Infrastructure.Persistence;
using TransactionFlow.Infrastructure.Persistence.Outbox;
using TransactionFlow.Worker.Kafka;

namespace TransactionFlow.Worker.Outbox;

public sealed class OutboxDispatcher(
    IServiceScopeFactory scopeFactory,
    IProducer<string, string> producer,
    IOptions<KafkaOptions> options,
    ILogger<OutboxDispatcher> logger)
    : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope =
                    scopeFactory.CreateScope();

                var db =
                    scope.ServiceProvider
                        .GetRequiredService<TransactionFlowDbContext>();

                var messages =
                    await db.OutboxMessages
                        .Where(x => x.PublishedAt == null)
                        .OrderBy(x => x.OccurredAt)
                        .Take(100)
                        .ToListAsync(stoppingToken);

                foreach (var message in messages)
                {
                    try
                    {
                        await producer.ProduceAsync(
                            options.Value.Topic,
                            new Message<string, string>
                            {
                                Key = message.Id.ToString(),
                                Value = message.Payload
                            },
                            stoppingToken);

                        message.PublishedAt =
                            DateTimeOffset.UtcNow;

                        message.Attempts++;

                        await db.SaveChangesAsync(
                            stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        message.Attempts++;
                        message.Error = ex.Message;

                        logger.LogError(
                            ex,
                            "Failed to publish outbox message. " +
                            "Id={Id}, Attempts={Attempts}",
                            message.Id,
                            message.Attempts);

                        await db.SaveChangesAsync(
                            stoppingToken);
                    }
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(500),
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
        }
    }
}
