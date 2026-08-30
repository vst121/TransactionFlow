using TransactionFlow.Contracts.Retry;

namespace TransactionFlow.Worker.Kafka;

public interface IRetryProducer
{
    Task PublishAsync(
        string key,
        RetryMessage retryMessage,
        CancellationToken cancellationToken);
}