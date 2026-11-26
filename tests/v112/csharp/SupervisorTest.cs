using System.Diagnostics;
using IpcProtocol;

// P2 回归：C# 宿主侧进程守护（退避重启 / 优雅停止）
int passed = 0, failed = 0;

void Check(string name, bool cond, string extra = "")
{
    if (cond) { passed++; Console.WriteLine($"  PASS {name}"); }
    else { failed++; Console.WriteLine($"  FAIL {name} {extra}"); }
}

var py = "python";
var crashes = new List<int>();
var delays = new List<int>();
var restarts = new List<int>();
var gaveUp = false;

var sup = new Supervisor(py, new[] { "-c", "import sys; sys.exit(3)" },
    restartDelayMs: 150, maxRestartDelayMs: 1000, maxRestarts: 2, resetAfterMs: 10000);
sup.OnCrash = e => crashes.Add(e.ExitCode);
sup.OnRestarting = e => delays.Add(e.DelayMs);
sup.OnRestarted = e => restarts.Add(e.Attempt);
sup.OnGiveUp = e => gaveUp = true;

sup.Start();
await Task.Delay(4000);

Check("crash observed twice", crashes.Count(c => c == 3) >= 2, string.Join(",", crashes));
Check("backoff grows", delays.Count >= 2 && delays[1] > delays[0], string.Join(",", delays));
Check("restarted events", restarts.Count >= 1, string.Join(",", restarts));
Check("give-up after max_restarts", gaveUp);
Check("not running after give-up", !sup.IsRunning);
await sup.StopAsync();

// 长驻子进程：StopAsync 必须立即结束且不产生 crash
var crashes2 = new List<int>();
var sup2 = new Supervisor(py, new[] { "-c", "import time; time.sleep(30)" }, restartDelayMs: 100);
sup2.OnCrash = e => crashes2.Add(e.ExitCode);
sup2.Start();
Check("long child running", sup2.IsRunning && sup2.Pid.HasValue, $"{sup2.Pid}");
await Task.Delay(500);
await sup2.StopAsync();
await Task.Delay(600);
Check("stop is graceful", !sup2.IsRunning);
Check("stop does not emit crash", crashes2.Count == 0, string.Join(",", crashes2));

Console.WriteLine($"\n结果: {passed} passed, {failed} failed");
Environment.Exit(failed == 0 ? 0 : 1);
