package ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 进程守护（v1.1.3 / P2）：对齐 NativeService 的退避重启语义。
 *
 * <p>- 子进程退出 → 指数退避重启（默认 500ms 起、上限 30s）；
 * <p>- 稳定运行 resetAfterMs 后才清零重启计数（避免"启动即崩"让退避退化）；
 * <p>- stop() 是显式停止：不触发崩溃事件，也不再重启；
 * <p>- 超过 maxRestarts 后只回调 onGiveUp。
 */
public class Supervisor {

    public static final class CrashEvent {
        public final int exitCode, crashes;
        public CrashEvent(int exitCode, int crashes) { this.exitCode = exitCode; this.crashes = crashes; }
    }

    public static final class RestartingEvent {
        public final int attempt, delayMs, exitCode;
        public RestartingEvent(int attempt, int delayMs, int exitCode) {
            this.attempt = attempt; this.delayMs = delayMs; this.exitCode = exitCode;
        }
    }

    public static final class RestartedEvent {
        public final int attempt, pid;
        public RestartedEvent(int attempt, int pid) { this.attempt = attempt; this.pid = pid; }
    }

    public static final class GiveUpEvent {
        public final int attempts, exitCode;
        public GiveUpEvent(int attempts, int exitCode) { this.attempts = attempts; this.exitCode = exitCode; }
    }

    private final List<String> command;
    private final Map<String, String> env;
    private final String workingDirectory;

    public final boolean autoRestart;
    public final long restartDelayMs;
    public final long maxRestartDelayMs;
    public final Integer maxRestarts;
    public final long resetAfterMs;
    public final long stopTimeoutMs;

    public Consumer<CrashEvent> onCrash;
    public Consumer<RestartingEvent> onRestarting;
    public Consumer<RestartedEvent> onRestarted;
    public Consumer<GiveUpEvent> onGiveUp;

    private final Object lock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ipc-supervisor");
        t.setDaemon(true);
        return t;
    });

    private Process process;
    private ScheduledFuture<?> resetTimer;
    private boolean intentionalStop;
    private int restartAttempts;
    private int crashCount;

    public Supervisor(List<String> command) {
        this(command, null, null, true, 500, 30000, null, 30000, 5000);
    }

    public Supervisor(List<String> command, Map<String, String> env, String workingDirectory,
                      boolean autoRestart, long restartDelayMs, long maxRestartDelayMs,
                      Integer maxRestarts, long resetAfterMs, long stopTimeoutMs) {
        if (command == null || command.isEmpty()) throw new IllegalArgumentException("command is required");
        this.command = new ArrayList<>(command);
        this.env = env;
        this.workingDirectory = workingDirectory;
        this.autoRestart = autoRestart;
        this.restartDelayMs = restartDelayMs;
        this.maxRestartDelayMs = maxRestartDelayMs;
        this.maxRestarts = maxRestarts;
        this.resetAfterMs = resetAfterMs;
        this.stopTimeoutMs = stopTimeoutMs;
    }

    public synchronized void start() {
        synchronized (lock) {
            if (process != null && process.isAlive()) return;
            intentionalStop = false;
        }
        spawn();
    }

    /** 显式停止：不触发崩溃事件，也不再重启。 */
    public void stop() {
        Process proc;
        synchronized (lock) {
            intentionalStop = true;
            if (resetTimer != null) { resetTimer.cancel(false); resetTimer = null; }
            proc = process;
            process = null;
        }
        if (proc == null) return;
        if (proc.isAlive()) {
            proc.destroy();
            try {
                if (!proc.waitFor(stopTimeoutMs, TimeUnit.MILLISECONDS)) {
                    proc.destroyForcibly();
                    proc.waitFor(2, TimeUnit.SECONDS);
                }
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                proc.destroyForcibly();
            }
        }
    }

    public boolean isRunning() {
        synchronized (lock) { return process != null && process.isAlive(); }
    }

    public Long pid() {
        synchronized (lock) { return process == null ? null : process.pid(); }
    }

    public int crashCount() { synchronized (lock) { return crashCount; } }

    public int restartAttempts() { synchronized (lock) { return restartAttempts; } }

    private void spawn() {
        ProcessBuilder pb = new ProcessBuilder(command);
        if (workingDirectory != null) pb.directory(new java.io.File(workingDirectory));
        if (env != null) pb.environment().putAll(env);
        pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process proc;
        try {
            proc = pb.start();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        synchronized (lock) { process = proc; }
        armResetTimer();
        proc.onExit().thenAccept(this::handleExit);
    }

    private void armResetTimer() {
        synchronized (lock) {
            if (resetTimer != null) resetTimer.cancel(false);
            resetTimer = scheduler.schedule(() -> {
                synchronized (lock) { restartAttempts = 0; resetTimer = null; }
            }, resetAfterMs, TimeUnit.MILLISECONDS);
        }
    }

    private void handleExit(Process proc) {
        int code = proc.exitValue();
        int attempt;
        int crashes;
        synchronized (lock) {
            if (intentionalStop) return;
            if (process == proc) process = null;
            crashCount++;
            crashes = crashCount;
        }
        safe(onCrash, new CrashEvent(code, crashes));
        if (!autoRestart) return;

        synchronized (lock) {
            if (maxRestarts != null && restartAttempts >= maxRestarts) {
                safe(onGiveUp, new GiveUpEvent(restartAttempts, code));
                return;
            }
            restartAttempts++;
            attempt = restartAttempts;
        }
        long delay = Math.min(restartDelayMs * (1L << Math.min(attempt - 1, 20)), maxRestartDelayMs);
        safe(onRestarting, new RestartingEvent(attempt, (int) delay, code));
        scheduler.schedule(() -> {
            synchronized (lock) {
                if (intentionalStop) return;
            }
            try {
                spawn();
            } catch (RuntimeException err) {
                return;
            }
            safe(onRestarted, new RestartedEvent(attempt, pid() == null ? 0 : pid().intValue()));
        }, delay, TimeUnit.MILLISECONDS);
    }

    private static <T> void safe(Consumer<T> cb, T arg) {
        if (cb == null) return;
        try { cb.accept(arg); } catch (RuntimeException ignored) { }
    }
}
