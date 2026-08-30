using Confluent.Kafka;
using TransactionFlow.Contracts.DeadLetter;

namespace TransactionFlow.Worker.Kafka;

public interface IDeadLetterProducer
{
    Task PublishAsync(
        ConsumeResult<string, string> result,
        Exception exception,
        CancellationToken cancellationToken);
}