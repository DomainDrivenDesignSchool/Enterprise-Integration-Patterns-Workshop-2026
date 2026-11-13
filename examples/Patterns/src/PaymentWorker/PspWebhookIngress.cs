using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Workshop.PaymentWorker;

public sealed record PspWebhookSettings(IReadOnlyDictionary<string, string> ProviderSecrets, TimeSpan AllowedClockSkew, int MaxBodyBytes);

public static class PspWebhookIngress
{
    private const string EventIdHeader = "X-Webhook-Id";
    private const string TimestampHeader = "X-Webhook-Timestamp";
    private const string SignatureHeader = "X-Webhook-Signature";

    public static void MapPspWebhook(this WebApplication app)
    {
        app.MapPost("/webhooks/psp/{provider}", ReceiveAsync);
        app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
    }

    private static async Task<IResult> ReceiveAsync(HttpContext context, string provider,
        IDbContextFactory<PaymentWebhookDbContext> contextFactory, PspWebhookSettings settings, ILoggerFactory loggerFactory)
    {
        var log = loggerFactory.CreateLogger("PspWebhookIngress");
        if (provider.Length is 0 or > 100 || provider.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '-' && c != '_'))
            return Results.BadRequest(new { error = "Invalid provider key." });
        if (!settings.ProviderSecrets.TryGetValue(provider, out var secret) || string.IsNullOrWhiteSpace(secret))
            return Results.NotFound();

        if (!context.Request.Headers.TryGetValue(EventIdHeader, out var eventIdHeader) ||
            !context.Request.Headers.TryGetValue(TimestampHeader, out var timestampHeader) ||
            !context.Request.Headers.TryGetValue(SignatureHeader, out var signatureHeader))
            return Results.BadRequest(new { error = "Missing webhook authentication headers." });

        var eventId = eventIdHeader.ToString();
        var timestampText = timestampHeader.ToString();
        if (eventId.Length is 0 or > 200 || !long.TryParse(timestampText, out var timestampSeconds))
            return Results.BadRequest(new { error = "Invalid webhook metadata." });

        var now = DateTimeOffset.UtcNow;
        DateTimeOffset timestamp;
        try { timestamp = DateTimeOffset.FromUnixTimeSeconds(timestampSeconds); }
        catch (ArgumentOutOfRangeException) { return Results.Unauthorized(); }
        if ((now - timestamp).Duration() > settings.AllowedClockSkew)
            return Results.Unauthorized();

        if (context.Request.ContentLength.HasValue && context.Request.ContentLength.Value > settings.MaxBodyBytes)
            return Results.StatusCode(StatusCodes.Status413PayloadTooLarge);
        await using var buffer = new MemoryStream(Math.Min(settings.MaxBodyBytes, 16 * 1024));
        var chunk = new byte[8192];
        while (true)
        {
            var read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted);
            if (read == 0) break;
            if (buffer.Length + read > settings.MaxBodyBytes)
                return Results.StatusCode(StatusCodes.Status413PayloadTooLarge);
            await buffer.WriteAsync(chunk.AsMemory(0, read), context.RequestAborted);
        }
        var raw = buffer.ToArray();
        if (!PspWebhookSignature.Verify(secret, timestampText, raw, signatureHeader.ToString()))
            return Results.Unauthorized();

        string eventType, idempotencyKey, providerReference, payloadEventId;
        DateTimeOffset occurredAt;
        try
        {
            using var json = JsonDocument.Parse(raw, new JsonDocumentOptions { MaxDepth = 16 });
            var root = json.RootElement;
            payloadEventId = Required(root, "providerEventId");
            if (!StringComparer.Ordinal.Equals(payloadEventId, eventId))
                return Results.BadRequest(new { error = "Webhook ID header does not match the signed payload." });
            eventType = Required(root, "eventType");
            idempotencyKey = Required(root, "idempotencyKey");
            providerReference = root.TryGetProperty("providerReference", out var reference) ? reference.GetString() ?? "" : "";
            occurredAt = root.TryGetProperty("occurredAtUtc", out var occurred) && occurred.TryGetDateTimeOffset(out var parsed)
                ? parsed : timestamp;
            if (eventType is not ("payment.authorized" or "payment.declined" or "refund.succeeded" or "refund.failed"))
                return Results.BadRequest(new { error = "Unsupported webhook event type." });
            if (idempotencyKey.Length is 0 or > 600 || providerReference.Length > 500)
                return Results.BadRequest(new { error = "Invalid webhook payload fields." });
        }
        catch (JsonException) { return Results.BadRequest(new { error = "Malformed webhook JSON." }); }
        catch (InvalidOperationException) { return Results.BadRequest(new { error = "Malformed webhook JSON fields." }); }

        var hash = Convert.ToHexString(SHA256.HashData(raw));
        await using var db = await contextFactory.CreateDbContextAsync(context.RequestAborted);
        db.WebhookInbox.Add(new PspWebhookInboxRow
        {
            Provider = provider,
            ProviderEventId = eventId,
            PayloadHash = hash,
            Payload = Encoding.UTF8.GetString(raw),
            EventType = eventType,
            IdempotencyKey = idempotencyKey,
            ProviderReference = providerReference,
            OccurredAtUtc = occurredAt,
            Status = "Pending"
            // received_at_utc and next_attempt_at_utc are assigned by SQL Server defaults.
        });

        try
        {
            await db.SaveChangesAsync(context.RequestAborted);
            return Results.Accepted(value: new { accepted = true, duplicate = false });
        }
        catch (DbUpdateException exception) when (IsUniqueViolation(exception))
        {
            await using var checkDb = await contextFactory.CreateDbContextAsync(context.RequestAborted);
            var existingHash = await checkDb.WebhookInbox.AsNoTracking()
                .Where(x => x.Provider == provider && x.ProviderEventId == eventId)
                .Select(x => x.PayloadHash)
                .SingleOrDefaultAsync(context.RequestAborted);
            if (StringComparer.Ordinal.Equals(existingHash, hash))
                return Results.Accepted(value: new { accepted = true, duplicate = true });
            log.LogWarning("PSP event id collision for provider {Provider}, event {EventId}", provider, eventId);
            return Results.Conflict(new { error = "Provider event ID was already used with a different payload." });
        }
    }

    private static bool IsUniqueViolation(DbUpdateException exception) =>
        exception.GetBaseException() is SqlException sqlException &&
        sqlException.Errors.Cast<SqlError>().Any(x => x.Number is 2601 or 2627);

    private static string Required(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()! : throw new JsonException($"Missing or invalid {name}.");
}

public static class PspWebhookSignature
{
    /// <summary>Sandbox/provider adapter contract: HMAC-SHA256 over UTF8(timestamp + ".") followed by the exact raw body bytes.</summary>
    public static bool Verify(string secret, string timestamp, ReadOnlySpan<byte> rawBody, string signature)
    {
        if (string.IsNullOrWhiteSpace(secret) || string.IsNullOrWhiteSpace(timestamp) || string.IsNullOrWhiteSpace(signature)) return false;
        if (signature.StartsWith("sha256=", StringComparison.OrdinalIgnoreCase)) signature = signature[7..];
        byte[] supplied;
        try { supplied = Convert.FromHexString(signature); }
        catch (FormatException) { return false; }
        if (supplied.Length != 32) return false;
        var prefix = Encoding.UTF8.GetBytes(timestamp + ".");
        var payload = new byte[prefix.Length + rawBody.Length];
        prefix.CopyTo(payload, 0); rawBody.CopyTo(payload.AsSpan(prefix.Length));
        var expected = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload);
        return CryptographicOperations.FixedTimeEquals(expected, supplied);
    }
}
