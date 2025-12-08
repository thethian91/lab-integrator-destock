
param(
  [Parameter(Mandatory=$true)][string]$Host,
  [Parameter(Mandatory=$true)][int]$Port,
  [Parameter(Mandatory=$true)][string]$Path
)

$bytes = [System.IO.File]::ReadAllBytes($Path)
$client = New-Object System.Net.Sockets.TcpClient
$client.Connect($Host, $Port)
$stream = $client.GetStream()
$stream.Write($bytes, 0, $bytes.Length)
$stream.Close()
$client.Close()
Write-Output "Sent $($bytes.Length) bytes to $Host:$Port"
