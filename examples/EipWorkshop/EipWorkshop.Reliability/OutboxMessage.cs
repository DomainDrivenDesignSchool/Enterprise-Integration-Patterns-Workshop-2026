using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;

namespace EipWorkshop.Reliability;

public sealed class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Type { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? PublishedAt { get; set; }
    public int Attempts { get; set; }
}
