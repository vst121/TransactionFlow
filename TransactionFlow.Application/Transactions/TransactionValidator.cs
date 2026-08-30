using TransactionFlow.Contracts.Transactions;

namespace TransactionFlow.Application.Transactions;

public sealed class TransactionValidator
{
    public ValidationResult Validate(
        TransactionMessage transaction)
    {
        if (string.IsNullOrWhiteSpace(transaction.TransactionId))
        {
            return ValidationResult.Invalid(
                "TransactionId is required.");
        }

        if (string.IsNullOrWhiteSpace(transaction.MerchantId))
        {
            return ValidationResult.Invalid(
                "MerchantId is required.");
        }

        if (transaction.Amount <= 0)
        {
            return ValidationResult.Invalid(
                "Amount must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(transaction.Currency))
        {
            return ValidationResult.Invalid(
                "Currency is required.");
        }

        if (transaction.Currency.Length != 3)
        {
            return ValidationResult.Invalid(
                "Currency must contain exactly 3 characters.");
        }

        if (string.IsNullOrWhiteSpace(transaction.Status))
        {
            return ValidationResult.Invalid(
                "Status is required.");
        }

        if (!IsValidStatus(transaction.Status))
        {
            return ValidationResult.Invalid(
                $"Unsupported transaction status: '{transaction.Status}'.");
        }

        return ValidationResult.Valid();
    }

    private static bool IsValidStatus(
        string status)
    {
        return status.Equals(
                   "SUCCESS",
                   StringComparison.OrdinalIgnoreCase)
               || status.Equals(
                   "FAILED",
                   StringComparison.OrdinalIgnoreCase)
               || status.Equals(
                   "PENDING",
                   StringComparison.OrdinalIgnoreCase);
    }
}
