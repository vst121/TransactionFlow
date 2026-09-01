using TransactionFlow.Application.Outbox;
using TransactionFlow.Contracts.Events;
using TransactionFlow.Infrastructure.Persistence.Repositories;

namespace TransactionFlow.Infrastructure.Persistence.Outbox;

public sealed class OutboxStore(
    OutboxMessageRepository repository)
    : IOutboxStore
{
    public Task AddAsync(
        TransactionProcessedEvent @event,
        CancellationToken cancellationToken)
    {
        return repository.AddAsync(
            @event,
            cancellationToken);
    }
}
