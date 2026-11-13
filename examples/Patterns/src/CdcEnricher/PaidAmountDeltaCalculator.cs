namespace Workshop.CdcEnricher;

/// <summary>Converts row snapshots into a financial delta so CDC UPDATEs do not count an order twice.</summary>
public static class PaidAmountDeltaCalculator
{
    public static decimal Calculate(string operation, string? beforeStatus, decimal beforeAmount,
        string? afterStatus, decimal afterAmount)
    {
        var wasPaid = beforeStatus is ("Paid" or "Captured" or "Completed");
        var isPaid = operation != "d" && afterStatus is ("Paid" or "Captured" or "Completed");
        return (isPaid ? afterAmount : 0m) - (wasPaid ? beforeAmount : 0m);
    }
}
