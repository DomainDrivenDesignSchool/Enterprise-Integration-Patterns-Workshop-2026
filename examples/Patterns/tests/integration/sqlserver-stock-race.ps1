[CmdletBinding()]
param(
  [string]$Server = 'localhost,1433',
  [string]$Database = 'WorkshopInventory',
  [string]$User = 'sa',
  [Parameter(Mandatory=$true)][string]$Password,
  [int]$Concurrency = 24
)
$ErrorActionPreference = 'Stop'
$sku = 'RACE-SKU-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
$setup = "INSERT dbo.stock_items(sku,available,reserved,version) VALUES(N'$sku',1,0,1);"
& sqlcmd -S $Server -d $Database -U $User -P $Password -b -Q $setup
if ($LASTEXITCODE -ne 0) { throw 'Could not seed race SKU.' }
$jobs = 1..$Concurrency | ForEach-Object {
  Start-Job -ArgumentList $Server,$Database,$User,$Password,$sku -ScriptBlock {
    param($s,$d,$u,$p,$k)
    $query = "SET NOCOUNT ON; UPDATE dbo.stock_items SET available=available-1,reserved=reserved+1,version=version+1 WHERE sku=N'$k' AND available>=1; SELECT @@ROWCOUNT;"
    & sqlcmd -S $s -d $d -U $u -P $p -b -h -1 -W -Q $query
    if ($LASTEXITCODE -ne 0) { throw 'sqlcmd worker failed' }
  }
}
$results = $jobs | Wait-Job | Receive-Job
$jobs | Remove-Job -Force
$winners = @($results | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ -eq '1' }).Count
$stateQuery = "SELECT CONCAT(available,',',reserved) FROM dbo.stock_items WHERE sku=N'$sku';"
$state = (& sqlcmd -S $Server -d $Database -U $User -P $Password -b -h -1 -W -Q $stateQuery | Select-Object -Last 1).Trim()
Write-Host "Concurrent attempts: $Concurrency; successful reservations: $winners; final available,reserved: $state"
if ($winners -ne 1 -or $state -ne '0,1') { throw 'Inventory race invariant failed: expected exactly one winner and final stock 0,1.' }
Write-Host 'PASS: no oversell under concurrent conditional updates.'
