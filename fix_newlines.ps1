$filePath = "Services\Ingestion\IngestService.cs"
$content = Get-Content $filePath -Raw
$content = $content -replace '\\n', "`n"
Set-Content $filePath $content -NoNewline
Write-Host "Fixed newlines in $filePath"
