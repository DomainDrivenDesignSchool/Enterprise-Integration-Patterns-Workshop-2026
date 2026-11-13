[CmdletBinding()]
param(
  [string]$Server = 'localhost,1433',
  [string]$User = 'sa',
  [Parameter(Mandatory=$true)][string]$Password,
  [string]$BaseUrl = 'http://localhost:5081',
  [Parameter(Mandatory=$true)][string]$Secret
)
$ErrorActionPreference = 'Stop'
if ($Secret.Length -lt 32) { throw 'Use a random webhook secret of at least 32 characters.' }
$orderId = [guid]::NewGuid().ToString()
$key = "webhook-smoke:$orderId"
$eventId = "evt_$([guid]::NewGuid().ToString('N'))"
$reference = "psp-smoke-$orderId"
$sql = @"
INSERT INTO dbo.payment_attempts(id,order_id,idempotency_key,amount,currency,status,created_at_utc)
VALUES(NEWID(),'$orderId','$key',12345.67,'IRR','Initiated',SYSUTCDATETIME());
"@
$sqlFile = Join-Path $env:TEMP "payment-webhook-$orderId.sql"
Set-Content -Path $sqlFile -Value $sql -Encoding UTF8
try {
  & sqlcmd -S $Server -U $User -P $Password -d WorkshopPayment -b -i $sqlFile
  if ($LASTEXITCODE -ne 0) { throw 'Could not seed payment attempt.' }
} finally { Remove-Item $sqlFile -ErrorAction SilentlyContinue }

$bodyObject = [ordered]@{
  providerEventId = $eventId
  eventType = 'payment.authorized'
  idempotencyKey = $key
  providerReference = $reference
  occurredAtUtc = [DateTime]::UtcNow.ToString('o')
}
$body = $bodyObject | ConvertTo-Json -Compress
$timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds().ToString()
$prefix = [Text.Encoding]::UTF8.GetBytes("$timestamp.")
$bodyBytes = [Text.Encoding]::UTF8.GetBytes($body)
$payload = [byte[]]::new($prefix.Length + $bodyBytes.Length)
[Array]::Copy($prefix,0,$payload,0,$prefix.Length)
[Array]::Copy($bodyBytes,0,$payload,$prefix.Length,$bodyBytes.Length)
$hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($Secret))
try { $signature = [Convert]::ToHexString($hmac.ComputeHash($payload)) } finally { $hmac.Dispose() }
$headers = @{
  'X-Webhook-Id' = $eventId
  'X-Webhook-Timestamp' = $timestamp
  'X-Webhook-Signature' = "sha256=$signature"
}
$url = "$($BaseUrl.TrimEnd('/'))/webhooks/psp/sandbox"
$first = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -Body $body
$duplicate = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -Body $body
if (-not $first.accepted -or $duplicate.duplicate -ne $true) { throw 'Webhook accept/dedup response did not match expectations.' }

$deadline = [DateTime]::UtcNow.AddSeconds(30)
$status = $null
while ([DateTime]::UtcNow -lt $deadline) {
  $query = "SELECT status FROM dbo.payment_attempts WHERE idempotency_key='$key';"
  $status = (& sqlcmd -S $Server -U $User -P $Password -d WorkshopPayment -h -1 -W -Q $query | Out-String).Trim()
  if ($status -eq 'Unknown') { break }
  Start-Sleep -Milliseconds 500
}
if ($status -ne 'Unknown') { throw "Webhook did not schedule provider inquiry; payment status is '$status'." }
$ledgerQuery = "SELECT COUNT(*) FROM dbo.payment_ledger WHERE order_id='$orderId' AND entry_type='Charge';"
$count = (& sqlcmd -S $Server -U $User -P $Password -d WorkshopPayment -h -1 -W -Q $ledgerQuery | Out-String).Trim()
if ($count -ne '0') { throw "Webhook callback must not directly write a financial ledger entry; found '$count'." }
Write-Host "PASS: webhook accepted once, duplicate deduplicated, attempt moved to Unknown for authoritative provider inquiry, no direct ledger write. Order=$orderId"
