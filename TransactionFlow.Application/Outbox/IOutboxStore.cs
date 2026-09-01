using TransactionFlow.Contracts.Events;

namespace TransactionFlow.Application.Outbox;

public interface IOutboxStore
{
    Task AddAsync(
        TransactionProcessedEvent @event,
        CancellationToken cancellationToken);
}
