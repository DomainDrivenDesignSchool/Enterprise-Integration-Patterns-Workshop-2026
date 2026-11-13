[CmdletBinding()]
param(
  [string]$Server = 'localhost,1433',
  [string]$User = 'sa',
  [Parameter(Mandatory=$true)][string]$Password
)
$ErrorActionPreference = 'Stop'
$script = Join-Path $PSScriptRoot '02-enable-orders-cdc.sql'
& sqlcmd -S $Server -U $User -P $Password -d WorkshopOrders -b -i $script
if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed with exit code $LASTEXITCODE" }
Write-Host 'CDC enabled for dbo.orders. Confirm SQL Server Agent capture job is running.'
