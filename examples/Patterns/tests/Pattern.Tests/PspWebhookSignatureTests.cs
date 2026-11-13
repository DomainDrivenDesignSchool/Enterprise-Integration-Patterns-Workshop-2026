using System.Security.Cryptography;
using System.Text;
using Workshop.PaymentWorker;
using Xunit;

namespace Workshop.Pattern.Tests;

public sealed class PspWebhookSignatureTests
{
    [Fact]
    public void Verify_AcceptsExactRawBodyAndTimestampSignature()
    {
        const string secret = "test-secret-with-enough-entropy";
        const string timestamp = "1794430000";
        var body = Encoding.UTF8.GetBytes("{\"eventType\":\"payment.authorized\"}");
        var signature = Sign(secret, timestamp, body);
        Assert.True(PspWebhookSignature.Verify(secret, timestamp, body, "sha256=" + signature));
    }

    [Fact]
    public void Verify_RejectsBodyTampering()
    {
        const string secret = "test-secret-with-enough-entropy";
        const string timestamp = "1794430000";
        var body = Encoding.UTF8.GetBytes("{\"amount\":100}");
        var signature = Sign(secret, timestamp, body);
        var tampered = Encoding.UTF8.GetBytes("{\"amount\":900}");
        Assert.False(PspWebhookSignature.Verify(secret, timestamp, tampered, signature));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-hex")]
    [InlineData("00")]
    public void Verify_RejectsMalformedSignatures(string signature) =>
        Assert.False(PspWebhookSignature.Verify("secret", "123", Encoding.UTF8.GetBytes("{}"), signature));

    private static string Sign(string secret, string timestamp, byte[] body)
    {
        var prefix = Encoding.UTF8.GetBytes(timestamp + ".");
        var payload = prefix.Concat(body).ToArray();
        return Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload));
    }
}
