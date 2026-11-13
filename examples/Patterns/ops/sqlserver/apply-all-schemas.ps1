[CmdletBinding()]
param(
  [string]$Server = 'localhost,1433',
  [string]$User = 'sa',
  [Parameter(Mandatory=$true)][string]$Password
)
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
& (Join-Path $root 'create-databases.ps1') -Server $Server -User $User -Password $Password
$steps = @(
  'apply-orders-schema.ps1',
  'apply-payment-schema.ps1',
  'apply-inventory-schema.ps1',
  'apply-saga-schema.ps1',
  'apply-shipping-schema.ps1',
  'apply-analytics-schema.ps1',
  'apply-retry-schema.ps1',
  'apply-stream-schema.ps1',
  'apply-cdc-enrichment-schema.ps1',
  'apply-event-store-schema.ps1'
)
foreach ($step in $steps) {
  Write-Host "Applying $step ..."
  & (Join-Path $root $step) -Server $Server -User $User -Password $Password
}
Write-Host 'All SQL Server service schemas applied. CDC enablement is a separate step.'
