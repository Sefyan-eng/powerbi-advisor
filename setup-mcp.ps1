# setup-mcp.ps1 - Download and install Power BI Modeling MCP Server
# Run in PowerShell (as Admin recommended): .\setup-mcp.ps1

$ErrorActionPreference = "Stop"

$version = "0.1.9"
$platform = "win32-x64"
$installDir = "C:\MCPServers\PowerBIModelingMCP"
$url = "https://marketplace.visualstudio.com/_apis/public/gallery/publishers/analysis-services/vsextensions/powerbi-modeling-mcp/$version/vspackage?targetPlatform=$platform"
$downloadPath = "$env:TEMP\powerbi-modeling-mcp-raw"
$vsixPath = "$env:TEMP\powerbi-modeling-mcp.vsix"
$extractDir = "$env:TEMP\powerbi-mcp-extract"

Write-Host ""
Write-Host "=== Power BI Modeling MCP Server Setup ===" -ForegroundColor Cyan
Write-Host "Version: $version | Platform: $platform" -ForegroundColor Gray
Write-Host ""

# Step 1: Download VSIX from VS Marketplace
Write-Host "[1/4] Downloading from VS Marketplace..." -ForegroundColor Yellow
$wc = New-Object System.Net.WebClient
$wc.DownloadFile($url, $downloadPath)
$size = [math]::Round((Get-Item $downloadPath).Length / 1MB, 1)
Write-Host "       Downloaded: $size MB" -ForegroundColor Green

# Step 2: Decompress (VS Marketplace serves gzip-compressed VSIX)
Write-Host "[2/4] Decompressing and extracting..." -ForegroundColor Yellow

# Check if gzip (first 2 bytes = 1F 8B)
$header = [System.IO.File]::ReadAllBytes($downloadPath)[0..1]
if ($header[0] -eq 0x1F -and $header[1] -eq 0x8B) {
    Write-Host "       Detected gzip compression, decompressing..." -ForegroundColor Gray
    $inStream = [System.IO.File]::OpenRead($downloadPath)
    $gzStream = New-Object System.IO.Compression.GzipStream($inStream, [System.IO.Compression.CompressionMode]::Decompress)
    $outStream = [System.IO.File]::Create($vsixPath)
    $gzStream.CopyTo($outStream)
    $outStream.Close()
    $gzStream.Close()
    $inStream.Close()
} else {
    # Already a plain ZIP/VSIX
    Copy-Item $downloadPath $vsixPath -Force
}

# Extract the VSIX (it's a ZIP file - rename for Expand-Archive compatibility)
if (Test-Path $extractDir) { Remove-Item $extractDir -Recurse -Force }
$zipPath = "$env:TEMP\powerbi-modeling-mcp.zip"
Copy-Item $vsixPath $zipPath -Force
Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force
Remove-Item $zipPath -Force -ErrorAction SilentlyContinue
Write-Host "       Extracted successfully" -ForegroundColor Green

# Step 3: Find the MCP server executable
Write-Host "[3/4] Locating MCP server executable..." -ForegroundColor Yellow
$exeFile = Get-ChildItem -Path $extractDir -Recurse -Filter "powerbi-modeling-mcp.exe" | Select-Object -First 1

if (-not $exeFile) {
    Write-Host ""
    Write-Host "  ERROR: Could not find powerbi-modeling-mcp.exe in the VSIX package." -ForegroundColor Red
    Write-Host "  Contents of extracted package:" -ForegroundColor Red
    Get-ChildItem -Path $extractDir -Recurse -File | Select-Object -First 30 | ForEach-Object { Write-Host "    $($_.FullName)" -ForegroundColor Gray }
    exit 1
}

Write-Host "       Found: $($exeFile.FullName)" -ForegroundColor Green

# Step 4: Copy server files to install directory
Write-Host "[4/4] Installing to $installDir ..." -ForegroundColor Yellow
if (Test-Path $installDir) { Remove-Item $installDir -Recurse -Force }
New-Item -ItemType Directory -Path $installDir -Force | Out-Null

# Copy the entire server directory (exe + dependencies like .dll, .node, etc.)
$serverDir = $exeFile.Directory
Copy-Item -Path "$($serverDir.FullName)\*" -Destination $installDir -Recurse -Force

$finalExe = Join-Path $installDir $exeFile.Name

# Verify
if (Test-Path $finalExe) {
    Write-Host ""
    Write-Host "  SUCCESS!" -ForegroundColor Green
    Write-Host "  MCP Server installed at:" -ForegroundColor Cyan
    Write-Host "  $finalExe" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Use this path in the Power BI Model Advisor app." -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "  ERROR: Installation failed - file not found at $finalExe" -ForegroundColor Red
    exit 1
}

# Cleanup
Remove-Item $downloadPath -Force -ErrorAction SilentlyContinue
Remove-Item $vsixPath -Force -ErrorAction SilentlyContinue
Remove-Item $extractDir -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "  Temp files cleaned up." -ForegroundColor Gray
Write-Host ""
