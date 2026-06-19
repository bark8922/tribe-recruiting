param([Parameter(Mandatory=$true)][string]$Token)
$ErrorActionPreference = "Stop"
$repo = "bark8922/tribe-circle"
$branch = "main"
$bytes = [IO.File]::ReadAllBytes("$PSScriptRoot\circle.html")
$b64 = [Convert]::ToBase64String($bytes)
$headers = @{
  Authorization = "token $Token"
  "User-Agent"  = "circle-push"
  Accept        = "application/vnd.github+json"
}
foreach ($path in @("circle.html","index.html")) {
  $url = "https://api.github.com/repos/$repo/contents/$path"
  $cur = Invoke-RestMethod -Uri ("{0}?ref={1}" -f $url, $branch) -Headers $headers
  $body = @{
    message = "Default to this_week + stop dropdown persisting period in URL"
    content = $b64
    sha     = $cur.sha
    branch  = $branch
  } | ConvertTo-Json
  Invoke-RestMethod -Uri $url -Method PUT -Headers $headers -Body $body -ContentType "application/json" | Out-Null
  Write-Host "pushed $path"
}
Write-Host "Cloudflare will auto-deploy in ~1 min."
