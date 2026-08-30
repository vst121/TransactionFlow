namespace TransactionFlow.Application.Common.Errors;

public interface IErrorClassifier
{
    ErrorKind Classify(Exception exception);
}