# v1.1.2 C# 侧鉴权/能力协商回归：起 Node 服务端 → 跑 AuthClient
param([string]$OutDir = "E:\ipcprotocol\_test-out")
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$root = 'E:\ipcprotocol'
Set-Location $root
$dotnet = 'C:\Program Files\dotnet\dotnet.exe'

& $dotnet build tests\v112\csharp\AuthClient.csproj -c Release -v q | Out-String | Write-Host

function Run-Case([string]$name, [string]$pipe, [string]$serverToken, [string]$clientToken, [string]$mode) {
    $env:PIPE_NAME = $pipe
    $env:IPC_TOKEN = $serverToken
    $srv = Start-Process -FilePath 'node' -ArgumentList 'tests\v112\node_auth_server.js' -PassThru -NoNewWindow `
        -RedirectStandardOutput "$OutDir\v112_cssrv_$name.log" -RedirectStandardError "$OutDir\v112_cssrv_$name.err.log"
    Start-Sleep -Milliseconds 700
    $code = 1
    try {
        $env:IPC_TOKEN = $clientToken
        $env:MODE = $mode
        $exe = "$root\tests\v112\csharp\bin\Release\net8.0\AuthClient.exe"
        $res = & $exe 2>&1
        $code = $LASTEXITCODE
        $res | ForEach-Object { Write-Host $_ }
        Set-Content -Path "$OutDir\v112_cs_$name.log" -Value $res
        Write-Host "  exit=$code"
    } finally {
        Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 200
    }
    return $code
}

Write-Host "`n=== C# 正确 token（从 IPC_TOKEN 环境变量继承）==="
$a = Run-Case 'good' "v112_cs_good_$(Get-Random)" 'cstok-456' 'cstok-456' 'good'

Write-Host "`n=== C# 错误 token ==="
$b = Run-Case 'bad' "v112_cs_bad_$(Get-Random)" 'cstok-456' 'wrong-token' 'bad'

if ($a -ne 0 -or $b -ne 0) { throw "C# 鉴权回归失败 a=$a b=$b" }
Write-Host "`nC# 鉴权回归全部通过"
