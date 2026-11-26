using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace IpcProtocol
{
    /// <summary>
    /// 进程守护（v1.1.3 / P2）：对齐 NativeService 的退避重启语义。
    ///
    /// - 子进程退出 → 指数退避重启（默认 500ms 起、上限 30s）；
    /// - 稳定运行 ResetAfterMs 后才清零重启计数（避免"启动即崩"让退避退化）；
    /// - StopAsync() 是显式停止：不触发崩溃事件，也不再重启；
    /// - 超过 MaxRestarts 后只回调 OnGiveUp。
    /// </summary>
    public sealed class Supervisor
    {
        public sealed record CrashEvent(int ExitCode, int Crashes, int Pid);
        public sealed record RestartingEvent(int Attempt, int DelayMs, int ExitCode);
        public sealed record RestartedEvent(int Attempt, int Pid);
        public sealed record GiveUpEvent(int Attempts, int ExitCode);
        public sealed record StartedEvent(int Pid, int Attempt);

        public Action<CrashEvent> OnCrash { get; set; }
        public Action<RestartingEvent> OnRestarting { get; set; }
        public Action<RestartedEvent> OnRestarted { get; set; }
        public Action<GiveUpEvent> OnGiveUp { get; set; }
        public Action<StartedEvent> OnStarted { get; set; }

        private readonly string _fileName;
        private readonly IReadOnlyList<string> _args;
        private readonly IReadOnlyDictionary<string, string> _env;
        private readonly string _workingDirectory;

        public int RestartDelayMs { get; }
        public int MaxRestartDelayMs { get; }
        public int? MaxRestarts { get; }
        public int ResetAfterMs { get; }
        public bool AutoRestart { get; }
        public int StopTimeoutMs { get; }

        private readonly object _lock = new object();
        private Process _process;
        private bool _intentionalStop;
        private bool _running;
        private int _restartAttempts;
        private int _crashCount;
        private CancellationTokenSource _resetCts;

        public Supervisor(string fileName, IReadOnlyList<string> args = null,
                          IReadOnlyDictionary<string, string> env = null, string workingDirectory = null,
                          bool autoRestart = true, int restartDelayMs = 500, int maxRestartDelayMs = 30000,
                          int? maxRestarts = null, int resetAfterMs = 30000, int stopTimeoutMs = 5000)
        {
            _fileName = fileName ?? throw new ArgumentNullException(nameof(fileName));
            _args = args ?? Array.Empty<string>();
            _env = env;
            _workingDirectory = workingDirectory;
            AutoRestart = autoRestart;
            RestartDelayMs = restartDelayMs;
            MaxRestartDelayMs = maxRestartDelayMs;
            MaxRestarts = maxRestarts;
            ResetAfterMs = resetAfterMs;
            StopTimeoutMs = stopTimeoutMs;
        }

        public int? Pid { get { lock (_lock) return _process?.Id; } }
        public bool IsRunning { get { lock (_lock) return _process != null && !_process.HasExited; } }
        public int CrashCount { get { lock (_lock) return _crashCount; } }
        public int RestartAttempts { get { lock (_lock) return _restartAttempts; } }

        public void Start()
        {
            lock (_lock)
            {
                if (_process != null && !_process.HasExited) return;
                _intentionalStop = false;
                _running = true;
            }
            Spawn();
        }

        /// <summary>显式停止：不触发崩溃事件，也不再重启。</summary>
        public async Task StopAsync()
        {
            Process proc;
            lock (_lock)
            {
                _intentionalStop = true;
                _running = false;
                _resetCts?.Cancel();
                proc = _process;
                _process = null;
            }
            if (proc == null || proc.HasExited) return;
            try
            {
                proc.Kill(entireProcessTree: true);
            }
            catch
            {
                // 已退出或不支持树杀，忽略
            }
            var wait = Task.Run(() => proc.WaitForExit());
            await Task.WhenAny(wait, Task.Delay(StopTimeoutMs));
        }

        private void Spawn()
        {
            var psi = new ProcessStartInfo(_fileName)
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = false,
                RedirectStandardError = false,
            };
            foreach (var a in _args) psi.ArgumentList.Add(a);
            if (_workingDirectory != null) psi.WorkingDirectory = _workingDirectory;
            if (_env != null)
            {
                foreach (var kv in _env) psi.Environment[kv.Key] = kv.Value;
            }
            var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };
            proc.Exited += (s, e) => _ = OnExited(proc);
            lock (_lock)
            {
                _process = proc;
            }
            proc.Start();
            ArmResetTimer();
            SafeInvoke(OnStarted, new StartedEvent(proc.Id, _restartAttempts));
        }

        private void ArmResetTimer()
        {
            lock (_lock)
            {
                _resetCts?.Cancel();
                var cts = new CancellationTokenSource();
                _resetCts = cts;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(ResetAfterMs, cts.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        return;
                    }
                    lock (_lock) _restartAttempts = 0;
                });
            }
        }

        private async Task OnExited(Process proc)
        {
            int code;
            try { code = proc.ExitCode; } catch { code = -1; }
            int pid;
            try { pid = proc.Id; } catch { pid = 0; }

            lock (_lock)
            {
                if (_intentionalStop || !_running) return;
                if (_process == proc) _process = null;
                _crashCount++;
            }
            int crashes;
            lock (_lock) crashes = _crashCount;
            SafeInvoke(OnCrash, new CrashEvent(code, crashes, pid));

            if (!AutoRestart) return;
            lock (_lock)
            {
                if (MaxRestarts.HasValue && _restartAttempts >= MaxRestarts.Value)
                {
                    SafeInvoke(OnGiveUp, new GiveUpEvent(_restartAttempts, code));
                    return;
                }
                _restartAttempts++;
            }

            int attempt;
            lock (_lock) attempt = _restartAttempts;
            int delay = Math.Min(RestartDelayMs * (int)Math.Pow(2, attempt - 1), MaxRestartDelayMs);
            SafeInvoke(OnRestarting, new RestartingEvent(attempt, delay, code));
            await Task.Delay(delay);
            lock (_lock)
            {
                if (_intentionalStop || !_running) return;
            }
            try
            {
                Spawn();
            }
            catch (Exception)
            {
                lock (_lock) _crashCount++;
                return; // 启动失败按崩溃退避，下一轮 Exited/调用方重试
            }
            SafeInvoke(OnRestarted, new RestartedEvent(attempt, Pid ?? 0));
        }

        private static void SafeInvoke<T>(Action<T> cb, T arg)
        {
            try { cb?.Invoke(arg); } catch { /* 宿主回调异常不影响守护 */ }
        }
    }
}
