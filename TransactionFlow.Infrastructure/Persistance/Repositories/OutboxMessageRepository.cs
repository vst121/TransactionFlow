using System.Text.Json;
using TransactionFlow.Contracts.Events;
using TransactionFlow.Infrastructure.Persistence.Outbox;

namespace TransactionFlow.Infrastructure.Persistence.Repositories;

public sealed class OutboxMessageRepository(
    TransactionFlowDbContext db)
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public Task AddAsync(
        TransactionProcessedEvent @event,
        CancellationToken cancellationToken)
    {
        var outboxMessage = new OutboxMessage
        {
            Id = Guid.Parse(@event.EventId),
            Type = nameof(TransactionProcessedEvent),
            Payload = JsonSerializer.Serialize(
                @event,
                JsonOptions),
            OccurredAt = @event.OccurredAt,
            Attempts = 0
        };

        db.OutboxMessages.Add(outboxMessage);

        return Task.CompletedTask;
    }
}
