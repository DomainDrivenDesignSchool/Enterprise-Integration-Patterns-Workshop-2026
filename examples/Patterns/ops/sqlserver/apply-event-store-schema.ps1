[CmdletBinding()]
param(
  [string]$Server = 'localhost,1433',
  [string]$User = 'sa',
  [Parameter(Mandatory=$true)][string]$Password
)
$ErrorActionPreference = 'Stop'
$script = Join-Path $PSScriptRoot '12-event-store-schema.sql'
& sqlcmd -S $Server -U $User -P $Password -d WorkshopEventStore -b -i $script
if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed with exit code $LASTEXITCODE" }
Write-Host 'WorkshopEventStore schema applied successfully.'
