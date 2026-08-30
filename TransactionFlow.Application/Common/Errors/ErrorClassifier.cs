using System.Text.Json;
using TransactionFlow.Application.Transactions;

namespace TransactionFlow.Application.Common.Errors;

public sealed class ErrorClassifier : IErrorClassifier
{
    public ErrorKind Classify(Exception exception)
    {
        return exception switch
        {
            InvalidTransactionException => ErrorKind.Permanent,
            JsonException => ErrorKind.Permanent,

            TimeoutException => ErrorKind.Transient,

            _ => ErrorKind.Transient
        };
    }
}