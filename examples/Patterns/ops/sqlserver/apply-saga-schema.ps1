[CmdletBinding()]
param(
  [string]$Server = 'localhost,1433',
  [string]$User = 'sa',
  [Parameter(Mandatory=$true)][string]$Password
)
$ErrorActionPreference = 'Stop'
$script = Join-Path $PSScriptRoot '09-saga-schema.sql'
& sqlcmd -S $Server -U $User -P $Password -d master -b -i $script
if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed with exit code $LASTEXITCODE" }
Write-Host 'WorkshopSaga schema applied successfully.'
