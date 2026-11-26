"""进程守护（v1.1.3 / P2）：非 Node 宿主拉起子进程时的指数退避重启。

与 `nativeService.js` 的守护语义保持一致，便于跨语言宿主复用同一套编排：

- 子进程退出 → 按退避重启（默认 500ms 起，上限 30s），管道名/端口不重建；
- 只有"连接稳定运行 reset_after 秒"后才清零重启计数，避免启动即崩导致退避被反复重置；
- ``stop()`` 是显式停止：不触发 crash，也不再重启；
- 超过 ``max_restarts`` 后只回调 ``on_give_up``，交由外层编排处理。

用法::

    sup = Supervisor(
        ["node", "child.js"],
        env={"PIPE_NAME": pipe, "IPC_TOKEN": token},
        on_crash=lambda info: print("crash", info),
        on_restarting=lambda info: print("restart in", info["delay_ms"]),
        on_restarted=lambda info: print("restarted", info),
        on_give_up=lambda info: print("give up", info),
    )
    sup.start()
    ...
    sup.stop()
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from typing import Callable, Mapping, Optional, Sequence


class Supervisor:
    """指数退避 + 优雅停止的子进程守护。"""

    def __init__(
        self,
        command: Sequence[str],
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        auto_restart: bool = True,
        restart_delay_ms: int = 500,
        max_restart_delay_ms: int = 30000,
        max_restarts: Optional[int] = None,
        reset_after_ms: int = 30000,
        stop_timeout_ms: int = 5000,
    ):
        if not command:
            raise ValueError("command is required")
        self.command = list(command)
        self.env = dict(os.environ, **(env or {}))
        self.cwd = cwd
        self.auto_restart = auto_restart
        self.restart_delay_ms = restart_delay_ms
        self.max_restart_delay_ms = max_restart_delay_ms
        self.max_restarts = max_restarts
        self.reset_after_ms = reset_after_ms
        self.stop_timeout_ms = stop_timeout_ms

        self.on_crash: Optional[Callable[[dict], None]] = None
        self.on_restarting: Optional[Callable[[dict], None]] = None
        self.on_restarted: Optional[Callable[[dict], None]] = None
        self.on_give_up: Optional[Callable[[dict], None]] = None
        self.on_started: Optional[Callable[[dict], None]] = None

        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._intentional_stop = False
        self._restart_attempts = 0
        self._crash_count = 0
        self._stable_timer: Optional[threading.Timer] = None
        self._watch_thread: Optional[threading.Thread] = None

    # ---------- 生命周期 ----------

    def start(self) -> subprocess.Popen:
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                return self._proc
            self._intentional_stop = False
            self._stop_event.clear()
            self._spawn()
            if self._watch_thread is None or not self._watch_thread.is_alive():
                self._watch_thread = threading.Thread(target=self._watch_loop, name="ipc-supervisor", daemon=True)
                self._watch_thread.start()
            return self._proc

    def stop(self) -> None:
        """显式停止：不触发 crash，也不再重启。"""
        with self._lock:
            self._intentional_stop = True
            self._stop_event.set()
            if self._stable_timer:
                self._stable_timer.cancel()
                self._stable_timer = None
            proc = self._proc
            self._proc = None
        if proc is None:
            return
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=self.stop_timeout_ms / 1000)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=2)
                except Exception:
                    pass

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def pid(self) -> Optional[int]:
        return self._proc.pid if self._proc else None

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self.is_running(),
                "pid": self.pid,
                "crashes": self._crash_count,
                "restartAttempts": self._restart_attempts,
                "intentionalStop": self._intentional_stop,
            }

    # ---------- 内部 ----------

    def _spawn(self) -> subprocess.Popen:
        proc = subprocess.Popen(
            self.command,
            env=self.env,
            cwd=self.cwd,
            stdin=subprocess.DEVNULL,
        )
        self._proc = proc
        # 连接/进程稳定运行 reset_after 后才清零退避计数
        if self._stable_timer:
            self._stable_timer.cancel()
        self._stable_timer = threading.Timer(self.reset_after_ms / 1000, self._mark_stable)
        self._stable_timer.daemon = True
        self._stable_timer.start()
        self._emit(self.on_started, {"pid": proc.pid, "attempt": self._restart_attempts})
        return proc

    def _mark_stable(self) -> None:
        with self._lock:
            self._restart_attempts = 0
            self._stable_timer = None

    def _watch_loop(self) -> None:
        while True:
            with self._lock:
                proc = self._proc
            if proc is None:
                if self._stop_event.wait(0.1):
                    return
                continue
            code = proc.wait()
            with self._lock:
                if self._stop_event.is_set() or self._intentional_stop:
                    return
                self._proc = None
                self._crash_count += 1
                crash = {"code": code, "crashes": self._crash_count, "pid": proc.pid}
            self._emit(self.on_crash, crash)
            if not self.auto_restart:
                return
            if self.max_restarts is not None and self._restart_attempts >= self.max_restarts:
                self._emit(self.on_give_up, {"attempts": self._restart_attempts, "exitCode": code})
                return
            self._restart_attempts += 1
            delay = min(self.restart_delay_ms * (2 ** (self._restart_attempts - 1)), self.max_restart_delay_ms)
            self._emit(self.on_restarting, {"attempt": self._restart_attempts, "delay": delay, "exitCode": code})
            if self._stop_event.wait(delay / 1000):
                return
            with self._lock:
                if self._intentional_stop or self._stop_event.is_set():
                    return
                try:
                    self._spawn()
                except Exception as err:  # 启动失败也按崩溃处理，继续退避
                    self._emit(self.on_crash, {"code": -1, "crashes": self._crash_count, "error": str(err)})
                    continue
            self._emit(self.on_restarted, {"attempt": self._restart_attempts, "pid": self.pid})

    @staticmethod
    def _emit(cb, payload) -> None:
        if cb is None:
            return
        try:
            cb(payload)
        except Exception:
            pass
