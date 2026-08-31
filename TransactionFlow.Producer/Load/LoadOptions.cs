namespace TransactionFlow.Producer.Load;

public sealed class LoadOptions
{
    public int Count { get; init; } = 100; 
    public int Rate { get; init; } = 10; 
    public int Concurrency { get; init; } = 16;
}