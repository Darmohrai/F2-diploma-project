# Runs the tests of the given modules several times in a row and stops at the first failure.
#
#   scripts\stability.ps1 <modules> <runs> [maven options]
#   scripts\stability.ps1 kurin-test,kurin-locks 20
#   scripts\stability.ps1 kurin-spring-boot-starter 20 -Pboot4
#
# The modules must be built first (mvnw.cmd install -DskipTests): only the tests are run, offline, without
# recompiling. Each run uses one test JVM per module with a bounded heap (profile "stability";
# -Dkurin.test.heap=256m to change it). When a run fails, the surefire reports of that run are copied to
# stability-failures\<time>-run<N>\<module> and the script exits with status 1.
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$Modules,
    [Parameter(Mandatory = $true, Position = 1)][int]$Runs,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$MavenOptions
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$log = [System.IO.Path]::GetTempFileName()

try {
    for ($run = 1; $run -le $Runs; $run++) {
        $arguments = @('-o', '-q', '-Pstability') + $MavenOptions + @('-pl', $Modules, 'surefire:test')
        & "$root\mvnw.cmd" @arguments *> $log
        if ($LASTEXITCODE -eq 0) {
            Write-Output "run $run/$Runs passed"
            continue
        }
        $target = Join-Path 'stability-failures' ("{0}-run{1}" -f (Get-Date -Format 'yyyyMMdd-HHmmss'), $run)
        foreach ($module in $Modules.Split(',')) {
            $reports = Join-Path $module 'target\surefire-reports'
            if (Test-Path $reports) {
                $destination = Join-Path $target $module
                New-Item -ItemType Directory -Force $destination | Out-Null
                Copy-Item -Recurse -Force (Join-Path $reports '*') $destination
            }
        }
        New-Item -ItemType Directory -Force $target | Out-Null
        Copy-Item $log (Join-Path $target 'maven.log')
        Write-Error "run $run/$Runs FAILED; reports and the Maven log are in $target"
        Select-String -Path $log -Pattern 'Tests run:.*(FAIL|ERROR)|ERROR\]   ' | Select-Object -First 20 | ForEach-Object { Write-Output $_.Line }
        exit 1
    }
    Write-Output "${Modules}: $Runs runs in a row passed"
}
finally {
    Remove-Item -Force $log -ErrorAction Ignore
}
