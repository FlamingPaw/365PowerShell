
<#
.SYNOPSIS
 Adaptive parallel SharePoint Online report with selectable units (MB or MiB):
 - Connects with Azure AD app-only (certificate thumbprint).
 - Enumerates tenant sites (optionally excluding OneDrive).
 - Processes sites in parallel with adaptive throttle (3–5 typical).
 - Computes First-stage recycle bin, Second-stage recycle bin, Preservation Hold Library sizes,
   and appends a TOTALS row.
 - Logs per-site elapsed time & flags likely throttling (429/503).
 - Allows selecting output units: MB (decimal SI, 1e6 bytes) or MiB (binary, 1024^2 bytes).

.DOCS & BACKGROUND
 - ForEach-Object -Parallel (PowerShell 7+):
   https://learn.microsoft.com/powershell/scripting/dev-cross-plat/performance/parallel-execution?view=powershell-7.5
 - PnP Connect-PnPOnline (returning a connection object for reuse):
   https://pnp.github.io/powershell/cmdlets/Connect-PnPOnline.html
 - Get-PnPFolderStorageMetric (storman.aspx-backed, not real-time):
   https://pnp.github.io/powershell/cmdlets/Get-PnPFolderStorageMetric.html
 - PowerShell numeric literals: 'mb' is MiB (1024^2), not decimal MB:
   https://learn.microsoft.com/powershell/module/microsoft.powershell.core/about/about_numeric_literals
#>

param(
    # Admin Center URL (host or full URL accepted)
    [Parameter(Mandatory = $true)]
    [string]$AdminUrl, # e.g., https://contoso-admin.sharepoint.com

    # Entra App Registration IDs
    [Parameter(Mandatory = $true)]
    [string]$ClientId, # e.g., f1f837b5-cfa8-4d16-959c-bfa28078cf0a
    [Parameter(Mandatory = $true)]
    [string]$Tenant,   # e.g., contoso.onmicrosoft.com (use domain)

    # Certificate thumbprint (Windows cert store)
    [Parameter(Mandatory = $true)]
    [string]$Thumbprint, # e.g., B05366DAA28D21907F211BCF57FEADC1F52231B2

    # Adaptive parallel settings
    [int]$StartThrottle = 4, # starting concurrency (3–5 typical)
    [int]$MinThrottle   = 3,
    [int]$MaxThrottle   = 10,
    [int]$BatchSize     = 60, # 3–4 batches for ~199 sites

    # Optional filters
    [switch]$IncludeOneDriveSites, # include -my sites if desired
    [string]$OnlySitesLike,        # e.g., "/sites/Wellness*"
    [string]$ExcludeSitesLike,     # e.g., "/sites/Archive*"

    # Unit selection (MB = decimal 10^6; MiB = binary 1024^2)
    [ValidateSet('MB','MiB')]
    [string]$Unit = 'MB',

    # Outputs
    [string]$OutputCsv = ".\SharePoint_RecycleBins_Retention.csv",
    [string]$ErrorCsv  = ".\SharePoint_RecycleBins_Retention_Errors.csv",
    [string]$PerfCsv   = ".\SharePoint_RecycleBins_Retention_Perf.csv"
)

$ErrorActionPreference = 'Stop'
$ProgressPreference    = 'SilentlyContinue'

# Require PowerShell 7+ for -Parallel
if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "This script requires PowerShell 7+ for -Parallel."
}

Import-Module PnP.PowerShell -ErrorAction Stop

# --- Unit constants & labels ---
# PowerShell's 1MB is MiB (1024^2). We expose MB (1,000,000) and MiB (1,048,576) explicitly.
$BYTES_PER_MB_DEC = 1000000.0     # MB (SI)
$BYTES_PER_MIB    = 1MB           # MiB (binary, 1024^2)
$divisor   = if ($Unit -eq 'MB') { $BYTES_PER_MB_DEC } else { $BYTES_PER_MIB }
$unitLabel = $Unit

# Precompute dynamic column names to keep downstream logic simple
$FSCol  = "FirstStageRecycleBin$unitLabel"
$SSCol  = "SecondStageRecycleBin$unitLabel"
$PHCol  = "PreservationHold$unitLabel"
$TOTCol = "TotalDeletedStorage$unitLabel"

function Convert-Url {
    param([string]$UrlOrHost)
    $normalized = if ($UrlOrHost -notmatch '^\w+://') { "https://$UrlOrHost" } else { $UrlOrHost }
    $normalized.TrimEnd('/')
}

$AdminUrlNormalized = Convert-Url $AdminUrl
$adminHost = ([Uri]$AdminUrlNormalized).Host
$baseHost  = $adminHost -replace '-admin',''

Write-Host "Admin URL normalized: $AdminUrlNormalized" -ForegroundColor Cyan
Write-Host "Admin host: $adminHost`n Base host: $baseHost" -ForegroundColor Cyan

# --- 1) Connect to Admin Center (certificate auth; set current context) ---
try {
    Connect-PnPOnline `
        -Url        $AdminUrlNormalized `
        -ClientId   $ClientId `
        -Tenant     $Tenant `
        -Thumbprint $Thumbprint `
        -ValidateConnection
    Write-Host "Connected: AzureADAppOnly / TenantAdmin (current context set)." -ForegroundColor Green
}
catch {
    throw "Admin connect failed: $($_.Exception.Message). Ensure public cert is uploaded to the app and SharePoint Application permissions (Sites.FullControl.All) granted with admin consent."
}

# --- 2) Enumerate sites & apply filters ---
$siteParams = @{ Detailed = $true }
if ($IncludeOneDriveSites) { $siteParams.IncludeOneDriveSites = $true }

try {
    $allSites = Get-PnPTenantSite @siteParams -ErrorAction Stop
}
catch {
    Write-Error ("Get-PnPTenantSite failed: {0}" -f $_.Exception.Message)
    Write-Host "Fix: Confirm SharePoint *Application* permissions (Sites.FullControl.All) and admin consent, then retry." -ForegroundColor Yellow
    return
}

if ($OnlySitesLike)    { $allSites = $allSites | Where-Object { $_.Url -like "*$OnlySitesLike*" } }
if ($ExcludeSitesLike) { $allSites = $allSites | Where-Object { $_.Url -notlike "*$ExcludeSitesLike*" } }
if (-not $IncludeOneDriveSites) { $allSites = $allSites | Where-Object { $_.Url -notlike "*-my.sharepoint.com*" } }

Write-Host ("Total sites to process: {0}" -f $allSites.Count) -ForegroundColor Yellow
if ($allSites.Count -eq 0) { Write-Warning "No sites match the selection." ; return }

# Throttling detection regex (case-insensitive; flexible spacing)
$throttleRe = '(?i)(\b429\b|\b503\b|Too\s*many\s*requests|Server\s*Too\s*Busy|throttl)'

# --- 3) Parallel worker (per site) ---
function Invoke-SiteBatch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][object[]]$Sites,
        [int]$Throttle = 4,
        [string]$ClientId,
        [string]$Tenant,
        [string]$Thumbprint
    )

    $Sites |
    ForEach-Object -Parallel {
        Import-Module PnP.PowerShell -ErrorAction Stop

        # Bring unit variables into runspace
        $divisor   = $using:divisor
        $unitLabel = $using:unitLabel

        # Dynamic column names inside the runspace as well
        $FSCol  = "FirstStageRecycleBin$unitLabel"
        $SSCol  = "SecondStageRecycleBin$unitLabel"
        $PHCol  = "PreservationHold$unitLabel"
        $TOTCol = "TotalDeletedStorage$unitLabel"

        $siteUrl = $_.Url
        $start   = Get-Date
        $tid     = [System.Threading.Thread]::CurrentThread.ManagedThreadId

        Write-Host ("[START] {0:HH:mm:ss.fff} TID={1} Site={2}" -f $start, $tid, $siteUrl)

        try {
            # IMPORTANT: Use explicit connection per runspace to avoid context bleed.
            $conn = Connect-PnPOnline `
                -Url        $siteUrl `
                -ClientId   $using:ClientId `
                -Tenant     $using:Tenant `
                -Thumbprint $using:Thumbprint `
                -ReturnConnection
        }
        catch {
            $end     = Get-Date
            $elapsed = [math]::Round(($end - $start).TotalSeconds, 2)

            # Build dynamic object with null size fields
            $props = [ordered]@{
                SiteUrl        = $siteUrl
                $FSCol         = $null
                $SSCol         = $null
                $PHCol         = $null
                $TOTCol        = $null
                Error          = $_.Exception.Message
                ElapsedSeconds = $elapsed
                ThreadId       = $tid
                Throttled      = ($_.Exception.Message -match $using:throttleRe)
            }
            New-Object psobject -Property $props | Write-Output

            Write-Host ("[END] {0:HH:mm:ss.fff} TID={1} Site={2} Elapsed={3}s (CONNECT ERROR)" -f $end,$tid,$siteUrl,$elapsed) -ForegroundColor DarkYellow
            return
        }

        try {
            # First-stage recycle bin
            $fsItems = Get-PnPRecycleBinItem -FirstStage -RowLimit 50000 -ErrorAction SilentlyContinue -Connection $conn
            $fsSizeBytes = ($fsItems | Measure-Object -Property Size -Sum).Sum
            if (-not $fsSizeBytes) { $fsSizeBytes = 0 }

            # Second-stage recycle bin
            $ssItems = Get-PnPRecycleBinItem -SecondStage -RowLimit 50000 -ErrorAction SilentlyContinue -Connection $conn
            $ssSizeBytes = ($ssItems | Measure-Object -Property Size -Sum).Sum
            if (-not $ssSizeBytes) { $ssSizeBytes = 0 }

            # Preservation Hold Library
            $phSizeBytes = 0
            $phList = Get-PnPList -Identity "Preservation Hold Library" -ErrorAction SilentlyContinue -Connection $conn
            if (-not $phList) { $phList = Get-PnPList -Identity "PreservationHoldLibrary" -ErrorAction SilentlyContinue -Connection $conn }

            if ($phList) {
                # Preferred: storage metrics (storman.aspx-backed; not real-time)
                $metric = Get-PnPFolderStorageMetric -List $phList.Title -ErrorAction SilentlyContinue -Connection $conn
                if ($metric -and $metric.TotalSize) {
                    $phSizeBytes = [int64]$metric.TotalSize
                }
                else {
                    # Fallback: enumerate files — convert ServerRelativeUrl to site-relative
                    $web = Get-PnPWeb -Includes ServerRelativeUrl -Connection $conn
                    $srvRel  = $phList.RootFolder.ServerRelativeUrl
                    $siteRel = $srvRel.Substring($web.ServerRelativeUrl.Length).TrimStart('/')

                    $items = Get-PnPFolderItem -FolderSiteRelativeUrl $siteRel -Recursive -ErrorAction SilentlyContinue -Connection $conn
                    $files = $items | Where-Object { $_.TypedObject -match 'File' }
                    $phSizeBytes = ($files | Measure-Object -Property Length -Sum).Sum
                    if (-not $phSizeBytes) { $phSizeBytes = 0 }
                }
            }

            $end     = Get-Date
            $elapsed = [math]::Round(($end - $start).TotalSeconds, 2)

            # Build dynamic object with unit-aware columns
            $props = [ordered]@{
                SiteUrl        = $siteUrl
                $FSCol         = [math]::Round(($fsSizeBytes / $divisor), 2)
                $SSCol         = [math]::Round(($ssSizeBytes / $divisor), 2)
                $PHCol         = [math]::Round(($phSizeBytes / $divisor), 2)
                $TOTCol        = [math]::Round((($fsSizeBytes + $ssSizeBytes + $phSizeBytes) / $divisor), 2)
                Error          = $null
                ElapsedSeconds = $elapsed
                ThreadId       = $tid
                Throttled      = $false
            }
            New-Object psobject -Property $props | Write-Output

            Write-Host ("[END] {0:HH:mm:ss.fff} TID={1} Site={2} Elapsed={3}s" -f $end, $tid, $siteUrl, $elapsed) -ForegroundColor DarkGreen
        }
        catch {
            $end     = Get-Date
            $elapsed = [math]::Round(($end - $start).TotalSeconds, 2)

            $props = [ordered]@{
                SiteUrl        = $siteUrl
                $FSCol         = $null
                $SSCol         = $null
                $PHCol         = $null
                $TOTCol        = $null
                Error          = $_.Exception.Message
                ElapsedSeconds = $elapsed
                ThreadId       = $tid
                Throttled      = ($_.Exception.Message -match $using:throttleRe)
            }
            New-Object psobject -Property $props | Write-Output

            Write-Host ("[END] {0:HH:mm:ss.fff} TID={1} Site={2} Elapsed={3}s (WORK ERROR)" -f $end,$tid,$siteUrl,$elapsed) -ForegroundColor DarkYellow
        }
    } -ThrottleLimit $Throttle
}

# --- 4) Helper to summarize a batch & suggest next throttle ---
function Get-BatchSummary {
    param(
        [Parameter(Mandatory=$true)][object[]]$BatchResults,
        [int]$ThrottleUsed,
        [int]$MinThrottle,
        [int]$MaxThrottle
    )

    $count     = $BatchResults.Count
    $errs      = ($BatchResults | Where-Object { $_.Error }).Count
    $throttled = ($BatchResults | Where-Object { $_.Throttled }).Count

    # Average (guard for empty)
    $avgElapsed = if ($count -gt 0) {
        [math]::Round((($BatchResults | Measure-Object -Property ElapsedSeconds -Average).Average), 2)
    } else { $null }

    # True median (guard for empty)
    $values = $BatchResults |
        Select-Object -ExpandProperty ElapsedSeconds |
        Where-Object { $_ -is [double] -or $_ -is [int] } |
        Sort-Object

    $c = $values.Count
    if ($c -eq 0) {
        $medElapsed = $null
    }
    elseif ($c % 2 -eq 1) {
        $medElapsed = [math]::Round($values[[math]::Floor($c/2)], 2)
    }
    else {
        $lower = $values[$c/2 - 1]
        $upper = $values[$c/2]
        $medElapsed = [math]::Round((($lower + $upper) / 2), 2)
    }

    # Adaptive throttle heuristic
    $next = $ThrottleUsed
    if ($throttled -ge [math]::Ceiling($count * 0.10) -or ($avgElapsed -ge 20)) {
        $next = [math]::Max($MinThrottle, $ThrottleUsed - 1)
    }
    elseif ($throttled -le [math]::Floor($count * 0.02) -and ($avgElapsed -lt 12)) {
        $next = [math]::Min($MaxThrottle, $ThrottleUsed + 1)
    }

    [pscustomobject]@{
        BatchCount      = $count
        Errors          = $errs
        ThrottledFlags  = $throttled
        AvgSeconds      = $avgElapsed
        MedSeconds      = $medElapsed
        ThrottleUsed    = $ThrottleUsed
        NextThrottle    = $next
    }
}

# --- 5) Adaptive loop over batches ---
$results  = New-Object System.Collections.Generic.List[object]
$errors   = New-Object System.Collections.Generic.List[object]
$perf     = New-Object System.Collections.Generic.List[object]
$throttle = $StartThrottle

Write-Host ("Starting adaptive run: Min={0}, Start={1}, Max={2}, BatchSize={3}, Unit={4}" -f $MinThrottle,$StartThrottle,$MaxThrottle,$BatchSize,$Unit) -ForegroundColor Cyan

for ($i = 0; $i -lt $allSites.Count; $i += $BatchSize) {
    $batchStart = $i
    $batchEnd   = [math]::Min($i + $BatchSize - 1, $allSites.Count - 1)
    $batch      = $allSites[$batchStart .. $batchEnd]

    Write-Host ("Processing batch {0}..{1} of {2} with Throttle={3}" -f $batchStart, $batchEnd, $allSites.Count, $throttle) -ForegroundColor Cyan

    $st = Get-Date
    $batchResults = Invoke-SiteBatch -Sites $batch -Throttle $throttle -ClientId $ClientId -Tenant $Tenant -Thumbprint $Thumbprint
    $et = Get-Date
    $batchElapsed = [math]::Round(($et - $st).TotalSeconds, 2)

    foreach ($row in $batchResults) {
        if ($row.Error) { $errors.Add($row) } else { $results.Add($row) }
    }

    $summary = Get-BatchSummary -BatchResults $batchResults -ThrottleUsed $throttle -MinThrottle $MinThrottle -MaxThrottle $MaxThrottle
    $summary | Add-Member -NotePropertyName BatchElapsedSeconds -NotePropertyValue $batchElapsed
    $summary | Add-Member -NotePropertyName BatchStartIndex     -NotePropertyValue $batchStart
    $summary | Add-Member -NotePropertyName BatchEndIndex       -NotePropertyValue $batchEnd
    $perf.Add($summary)

    Write-Host ("Batch {0}-{1}: Elapsed={2}s `n Sites={3} `n Errors={4} `n ThrottledFlags={5} `n Avg={6}s `n Med={7}s `n NextThrottle={8}" -f `
        $batchStart,$batchEnd,$batchElapsed,$summary.BatchCount,$summary.Errors,$summary.ThrottledFlags,$summary.AvgSeconds,$summary.MedSeconds,$summary.NextThrottle) -ForegroundColor Yellow

    $throttle = $summary.NextThrottle
}

# --- 6) Totals row & outputs (unit-aware) ---
$fsTotal  = ($results | Measure-Object -Property $FSCol  -Sum).Sum
$ssTotal  = ($results | Measure-Object -Property $SSCol  -Sum).Sum
$phTotal  = ($results | Measure-Object -Property $PHCol  -Sum).Sum
$delTotal = ($results | Measure-Object -Property $TOTCol -Sum).Sum

$totalsRowProps = [ordered]@{
    SiteUrl        = 'TOTALS'
    $FSCol         = [math]::Round(($fsTotal), 2)
    $SSCol         = [math]::Round(($ssTotal), 2)
    $PHCol         = [math]::Round(($phTotal), 2)
    $TOTCol        = [math]::Round(($delTotal), 2)
    Error          = $null
    ElapsedSeconds = $null
    ThreadId       = $null
    Throttled      = $false
}
$totalsRow = New-Object psobject -Property $totalsRowProps

$display = $results + $totalsRow

# Console + CSVs
$display | Sort-Object SiteUrl | Format-Table -AutoSize

Write-Host ("`nTotals ({0}) -> FirstStage: {1} {0} `n SecondStage: {2} {0} `n PreservationHold: {3} {0} `n Combined: {4} {0}" -f `
    $unitLabel, `
    $totalsRow.$FSCol, `
    $totalsRow.$SSCol, `
    $totalsRow.$PHCol, `
    $totalsRow.$TOTCol) -ForegroundColor Green

$display | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

if ($errors.Count -gt 0) {
    $errors | Export-Csv -Path $ErrorCsv -NoTypeInformation -Encoding UTF8
    Write-Warning ("{0} site(s) failed; details saved to: {1}" -f $errors.Count, $ErrorCsv)
} else {
    Write-Host "No errors encountered." -ForegroundColor Green
}

$perf | Export-Csv -Path $PerfCsv -NoTypeInformation -Encoding UTF8

Write-Host ("Saved report to: {0}" -f $OutputCsv) -ForegroundColor Green
Write-Host ("Saved batch perf to: {0}" -f $PerfCsv) -ForegroundColor Green
