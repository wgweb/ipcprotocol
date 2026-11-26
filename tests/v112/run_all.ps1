# v1.1.2 / P1 一键回归：能力协商 + 默认鉴权 + 进程守护 + 多客户端 + token 轮换（含 v1.1 基线）
$ErrorActionPreference = 'Stop'
Set-Location E:\ipcprotocol
New-Item -ItemType Directory -Force -Path E:\ipcprotocol\_test-out | Out-Null

Write-Host "`n=== 1/5 Node：鉴权 + 能力协商 ===" -ForegroundColor Cyan
node tests\v112\test_auth_caps.js
if ($LASTEXITCODE -ne 0) { throw 'test_auth_caps 失败' }

Write-Host "`n=== 2/5 Node：进程守护（崩溃→退避重启→重连→优雅停止）===" -ForegroundColor Cyan
node tests\v112\test_restart.js
if ($LASTEXITCODE -ne 0) { throw 'test_restart 失败' }

Write-Host "`n=== 3/5 Node：多客户端会话 + token 热轮换 ===" -ForegroundColor Cyan
node tests\v112\test_multiclient_rotation.js
if ($LASTEXITCODE -ne 0) { throw 'test_multiclient_rotation 失败' }

Write-Host "`n=== 4/5 Python：token 继承 + 自动 hello ===" -ForegroundColor Cyan
pwsh -NoProfile -File tests\v112\run_py_auth.ps1
if ($LASTEXITCODE -ne 0) { throw 'Python 鉴权回归失败' }

Write-Host "`n=== 5/5 C#：token 继承 + 自动 hello ===" -ForegroundColor Cyan
pwsh -NoProfile -File tests\v112\run_cs_auth.ps1
if ($LASTEXITCODE -ne 0) { throw 'C# 鉴权回归失败' }

Write-Host "`n=== 基线回归：v1.1 心跳/鉴权/重同步 ===" -ForegroundColor Cyan
node test_v11.js
if ($LASTEXITCODE -ne 0) { throw 'test_v11 失败' }

Write-Host "`n=== P2：宿主侧守护（Python / C# / Java）===" -ForegroundColor Cyan
python tests\v112\test_supervisor.py
if ($LASTEXITCODE -ne 0) { throw 'Python 守护回归失败' }
& "C:\Program Files\dotnet\dotnet.exe" run --project tests\v112\csharp\SupervisorTest.csproj -c Release --nologo 2>&1 | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'C# 守护回归失败' }
java -cp java\out ipc.SupervisorTest
if ($LASTEXITCODE -ne 0) { throw 'Java 守护回归失败' }

Write-Host "`n五语言 P1 自测（Java/Go/Rust/C++/C）" -ForegroundColor Cyan
javac -encoding UTF-8 -d java\out java\src\ipc\*.java
java -cp java\out ipc.LoopbackTest
if ($LASTEXITCODE -ne 0) { throw 'Java LoopbackTest 失败' }
Push-Location go; go test -count=1 ./...; $goExit = $LASTEXITCODE; Pop-Location
if ($goExit -ne 0) { throw 'Go test 失败' }
Push-Location rust; cargo test --offline 2>&1 | Out-Host; $rsExit = $LASTEXITCODE; Pop-Location
if ($rsExit -ne 0) { throw 'Rust test 失败' }
g++ -std=c++17 -O2 -Wall -Icpp/include cpp/tests/loopback_test.cpp -o _test-out\cpp_lb.exe -lws2_32
_test-out\cpp_lb.exe
if ($LASTEXITCODE -ne 0) { throw 'C++ loopback 失败' }
gcc -std=c11 -O2 -Wall -Ic c\loopback_test.c c\json.c c\message_stream.c c\rpc.c c\tcp.c -o _test-out\c_lb.exe -lws2_32 -lpthread
_test-out\c_lb.exe
if ($LASTEXITCODE -ne 0) { throw 'C loopback 失败' }

Write-Host "`nv1.1.2 + P1 + P2 全部回归通过" -ForegroundColor Green

