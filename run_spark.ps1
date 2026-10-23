
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Script
)

$ProjectPath = $PSScriptRoot
$BronzePath = Join-Path $ProjectPath "data\raw\pypi"

if (-not (Test-Path (Join-Path $ProjectPath $Script))) {
    Write-Error "Script not found: $Script"
    exit 1
}

docker run --rm `
    -v "${ProjectPath}:/opt/project" `
    -v "${BronzePath}:/opt/project/data/raw/pypi:ro" `
    -w /opt/project `
    osd-spark:4.2.0 `
    /opt/spark/bin/spark-submit $Script

if ($LASTEXITCODE -ne 0) {
    Write-Error "Spark job failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
