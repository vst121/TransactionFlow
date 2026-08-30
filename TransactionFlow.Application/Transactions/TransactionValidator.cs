using System.ComponentModel.DataAnnotations;
using TransactionFlow.Contracts;

namespace TransactionFlow.Application.Transactions;

public sealed class TransactionValidator
{
    public ValidationResult Validate(TransactionMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.TransactionId))
        {
            return ValidationResult.Invalid(
                "TransactionId is required.");
        }

        if (string.IsNullOrWhiteSpace(message.MerchantId))
        {
            return ValidationResult.Invalid(
                "MerchantId is required.");
        }

        if (message.Amount <= 0)
        {
            return ValidationResult.Invalid(
                "Amount must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(message.Currency))
        {
            return ValidationResult.Invalid(
                "Currency is required.");
        }

        if (message.Status is not ("SUCCESS" or "FAILED"))
        {
            return ValidationResult.Invalid(
                $"Unsupported status: {message.Status}");
        }

        return ValidationResult.Valid();
    }
}

public sealed record ValidationResult(
    bool IsValid,
    string? Error)
{
    public static ValidationResult Valid()
        => new(true, null);

    public static ValidationResult Invalid(string error)
        => new(false, error);
}