# Check for unreachable functions in examples

$out = deadcode ./cmd/... 2>&1

if ($LASTEXITCODE -ne 0) {
  $out | Out-String -Stream | ForEach-Object { Write-Host $_ }
  exit 1
}

if ($out) {
  Write-Host "dead code found:"
  $out | Out-String -Stream | ForEach-Object { Write-Host $_ }
  exit 1
}

Write-Host "no dead code found"
