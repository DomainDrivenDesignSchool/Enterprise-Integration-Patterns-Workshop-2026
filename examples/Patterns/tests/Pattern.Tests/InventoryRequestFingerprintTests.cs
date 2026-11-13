using Workshop.InventoryWorker;
using Xunit;

namespace Workshop.Pattern.Tests;

public sealed class InventoryRequestFingerprintTests
{
    [Fact]
    public void Normalize_IsIndependentOfInputOrder_AndCombinesDuplicateSkus()
    {
        var first = InventoryRequestFingerprint.Normalize(new[]
        {
            new InventoryLine("SKU-B", 1), new InventoryLine("SKU-A", 2), new InventoryLine("SKU-A", 3)
        });
        var second = InventoryRequestFingerprint.Normalize(new[]
        {
            new InventoryLine("SKU-A", 5), new InventoryLine("SKU-B", 1)
        });

        Assert.Equal(first, second);
        Assert.Equal("SKU-A", first[0].Sku);
        Assert.Equal(5, first[0].Quantity);
    }

    [Fact]
    public void Compute_ChangesWhenRequestedQuantityChanges()
    {
        var one = InventoryRequestFingerprint.Normalize(new[] { new InventoryLine("SKU-A", 1) });
        var two = InventoryRequestFingerprint.Normalize(new[] { new InventoryLine("SKU-A", 2) });

        Assert.NotEqual(InventoryRequestFingerprint.Compute(one), InventoryRequestFingerprint.Compute(two));
    }

    [Fact]
    public void Normalize_RejectsNonPositiveQuantity()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            InventoryRequestFingerprint.Normalize(new[] { new InventoryLine("SKU-A", 0) }));
    }
}
