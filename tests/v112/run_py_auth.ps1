# v1.1.2 Python 侧鉴权/能力协商回归驱动：起 Node 服务端 → 跑 py_auth_client
param([string]$OutDir = "E:\ipcprotocol\_test-out")
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$root = 'E:\ipcprotocol'
Set-Location $root

function Run-Case([string]$name, [string]$pipe, [string]$serverToken, [string]$clientToken, [string]$mode) {
    $env:PIPE_NAME = $pipe
    $env:IPC_TOKEN = $serverToken
    $srv = Start-Process -FilePath 'node' -ArgumentList 'tests\v112\node_auth_server.js' -PassThru -NoNewWindow `
        -RedirectStandardOutput "$OutDir\v112_pysrv_$name.log" -RedirectStandardError "$OutDir\v112_pysrv_$name.err.log"
    Start-Sleep -Milliseconds 700
    try {
        $env:IPC_TOKEN = $clientToken
        $env:MODE = $mode
        $script:res = & python tests\v112\py_auth_client.py 2>&1
        $code = $LASTEXITCODE
        $script:res | ForEach-Object { Write-Host $_ }
        Set-Content -Path "$OutDir\v112_py_$name.log" -Value $script:res
        Write-Host "  exit=$code"
    } finally {
        Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 200
    }
    return $code
}

Write-Host "`n=== A) Python 正确 token（来自 IPC_TOKEN 环境变量）==="
$a = Run-Case 'good' "v112_py_good_$(Get-Random)" 'pytok-123' 'pytok-123' 'good'

Write-Host "`n=== B) Python 错误 token ==="
$b = Run-Case 'bad' "v112_py_bad_$(Get-Random)" 'pytok-123' 'wrong-token' 'bad'

if ($a -ne 0 -or $b -ne 0) { throw "Python 鉴权回归失败 a=$a b=$b" }
Write-Host "`nPython 鉴权回归全部通过"


