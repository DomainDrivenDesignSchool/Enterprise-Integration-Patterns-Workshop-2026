using System.Security.Cryptography;
using System.Text;

namespace Workshop.InventoryWorker;

public static class InventoryRequestFingerprint
{
    public static IReadOnlyList<InventoryLine> Normalize(IEnumerable<InventoryLine> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);
        return lines
            .Select(line =>
            {
                if (string.IsNullOrWhiteSpace(line.Sku)) throw new ArgumentException("SKU is required.", nameof(lines));
                if (line.Sku.Trim().Length > 100) throw new ArgumentException("SKU cannot exceed 100 characters.", nameof(lines));
                if (line.Quantity <= 0) throw new ArgumentOutOfRangeException(nameof(lines), "Quantity must be positive.");
                return line;
            })
            .GroupBy(line => line.Sku.Trim(), StringComparer.Ordinal)
            .Select(group => new InventoryLine(group.Key, checked(group.Sum(line => line.Quantity))))
            .OrderBy(line => line.Sku, StringComparer.Ordinal)
            .ToArray();
    }

    public static string Compute(IEnumerable<InventoryLine> normalizedLines)
    {
        ArgumentNullException.ThrowIfNull(normalizedLines);
        var canonical = string.Join("\n", normalizedLines.Select(line => $"{line.Sku}\t{line.Quantity}"));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }
}
