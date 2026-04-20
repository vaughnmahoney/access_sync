# Copies this repo's sync_jobs folder into another project (e.g. OptimaFlow).
# Composite-key specs require matching spec_types.py, access_io.py, etc.
#
# Usage (PowerShell):
#   cd ...\access_sync
#   .\copy_sync_jobs.ps1 -Destination "C:\Users\ws14\Desktop\OptimaFlow"

param(
    [Parameter(Mandatory = $true)]
    [string] $Destination
)

$here = $PSScriptRoot
$src = Join-Path $here "sync_jobs"
$dst = Join-Path $Destination "sync_jobs"

if (-not (Test-Path $src)) {
    Write-Error "Source not found: $src (run from access_sync folder)"
    exit 1
}

New-Item -ItemType Directory -Force -Path $Destination | Out-Null
robocopy $src $dst /MIR /NFL /NDL /NJH /NJS | Out-Host
if ($LASTEXITCODE -ge 8) {
    Write-Error "robocopy failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
Write-Host "Updated: $dst"
