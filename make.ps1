<#
.SYNOPSIS
    RaceStream developer commands for Windows.

.DESCRIPTION
    A PowerShell equivalent of the Makefile, because `make` is not present on a
    default Windows install and a contributor should not have to install a
    toolchain before they can start the stack. The two are kept in step; the
    Makefile remains the reference on Linux and macOS.

.EXAMPLE
    ./make.ps1 dev
    ./make.ps1 logs api
    ./make.ps1 ingest 9158
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Target = 'help',

    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$Python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'

function Invoke-Compose { docker compose @args }

function Show-Help {
    Write-Host ''
    Write-Host '  RaceStream commands' -ForegroundColor White
    Write-Host ''
    $rows = @(
        @('dev',        'Build and start the whole stack'),
        @('up',         'Start without rebuilding'),
        @('infra',      'Start only database, broker and observability'),
        @('down',       'Stop containers, keep volumes'),
        @('ps',         'Container status'),
        @('logs',       'Tail logs:  ./make.ps1 logs api'),
        @('restart',    'Restart one service:  ./make.ps1 restart processor'),
        @('build',      'Rebuild images'),
        @('clean',      'Stop and remove volumes (destroys ingested data)'),
        @('test',       'Run the whole test suite'),
        @('test-py',    'Python tests only'),
        @('test-js',    'Frontend tests only'),
        @('lint',       'Lint Python and TypeScript'),
        @('format',     'Auto-format Python'),
        @('typecheck',  'Type-check both sides'),
        @('health',     'Print API health'),
        @('metrics',    'Print RaceStream metrics'),
        @('db',         'Row counts per table'),
        @('psql',       'Open a psql shell'),
        @('topics',     'List Kafka topics'),
        @('lag',        'Show consumer group lag'),
        @('ingest',     'Ingest a session:  ./make.ps1 ingest 9158'),
        @('replay',     'Replay a session:  ./make.ps1 replay 9158 2'),
        @('benchmark',  'Run the load test'),
        @('chaos',      'Stop the telemetry consumer to show recovery'),
        @('venv',       'Create the local Python environment')
    )
    foreach ($r in $rows) {
        Write-Host ('    {0,-12} ' -f $r[0]) -ForegroundColor Cyan -NoNewline
        Write-Host $r[1] -ForegroundColor Gray
    }
    Write-Host ''
}

function Show-Endpoints {
    Write-Host ''
    Write-Host '  Frontend    http://localhost:5173' -ForegroundColor Gray
    Write-Host '  API docs    http://localhost:8000/docs' -ForegroundColor Gray
    Write-Host '  Redpanda    http://localhost:8080' -ForegroundColor Gray
    Write-Host '  Prometheus  http://localhost:9090' -ForegroundColor Gray
    Write-Host '  Grafana     http://localhost:3002' -ForegroundColor Gray
    Write-Host ''
}

switch ($Target) {
    'help'  { Show-Help }

    'dev'   { Invoke-Compose up --build -d; Show-Endpoints }
    'up'    { Invoke-Compose up -d; Show-Endpoints }
    'infra' { Invoke-Compose up -d timescaledb redpanda redpanda-console prometheus grafana }
    'down'  { Invoke-Compose down }
    'ps'    { Invoke-Compose ps }
    'build' { Invoke-Compose build }
    'clean' { Invoke-Compose down -v }

    'logs' {
        if ($Args) { Invoke-Compose logs -f $Args[0] } else { Invoke-Compose logs -f }
    }
    'restart' {
        if (-not $Args) { throw 'Usage: ./make.ps1 restart <service>' }
        Invoke-Compose restart $Args[0]
    }

    'test'      { & $PSCommandPath 'test-py'; & $PSCommandPath 'test-js' }
    'test-py'   { & $Python -m pytest tests -v }
    'test-js'   { Push-Location frontend; try { npm test } finally { Pop-Location } }
    'lint' {
        & $Python -m ruff check services tests
        Push-Location frontend; try { npm run lint } finally { Pop-Location }
    }
    'format' {
        & $Python -m ruff format services tests
        & $Python -m ruff check --fix services tests
    }
    'typecheck' {
        & $Python -m mypy services/common/racestream_common
        Push-Location frontend; try { npm run typecheck } finally { Pop-Location }
    }

    'health'  { (Invoke-WebRequest -UseBasicParsing http://localhost:8000/api/system/health).Content | & $Python -m json.tool }
    'metrics' { (Invoke-WebRequest -UseBasicParsing http://localhost:8000/metrics).Content -split "`n" | Select-String '^racestream_' | Select-Object -First 60 }
    'psql'    { Invoke-Compose exec timescaledb psql -U racestream -d racestream }
    'topics'  { Invoke-Compose exec redpanda rpk topic list }
    'lag'     { Invoke-Compose exec redpanda rpk group list }

    'db' {
        $sql = @'
SELECT 'sessions' t, count(*) FROM sessions
UNION ALL SELECT 'drivers', count(*) FROM drivers
UNION ALL SELECT 'laps', count(*) FROM laps
UNION ALL SELECT 'car_telemetry', count(*) FROM car_telemetry
UNION ALL SELECT 'positions', count(*) FROM positions
UNION ALL SELECT 'timing', count(*) FROM timing
UNION ALL SELECT 'weather', count(*) FROM weather
UNION ALL SELECT 'race_control_events', count(*) FROM race_control_events;
'@
        Invoke-Compose exec -T timescaledb psql -U racestream -d racestream -c $sql
    }

    'ingest' {
        if (-not $Args) { throw 'Usage: ./make.ps1 ingest <session_id>' }
        Invoke-Compose run --rm ingestion python -m racestream_ingestion.main --session $Args[0]
    }

    'replay' {
        if (-not $Args) { throw 'Usage: ./make.ps1 replay <session_id> [speed]' }
        $speed = if ($Args.Count -ge 2) { $Args[1] } else { 1 }
        $body = @{ session_id = [int]$Args[0]; speed = [double]$speed } | ConvertTo-Json -Compress
        Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/replay/start `
            -ContentType 'application/json' -Body $body | ConvertTo-Json -Depth 5
    }

    'benchmark' { & $Python load/run_benchmark.py }
    'chaos'     { Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/chaos/consumer/stop | ConvertTo-Json -Depth 5 }

    'venv' {
        python -m venv .venv
        & $Python -m pip install --upgrade pip
        & $Python -m pip install -e services/common
        & $Python -m pip install -r services/api/requirements.txt
        & $Python -m pip install -r services/ingestion/requirements.txt
        & $Python -m pip install -r requirements-dev.txt
    }

    default {
        Write-Host "Unknown target: $Target" -ForegroundColor Red
        Show-Help
        exit 1
    }
}
