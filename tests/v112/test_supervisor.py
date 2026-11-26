"""P2 回归：Python 宿主侧进程守护（退避重启 / 优雅停止）。

用两个立刻 exit(3) 的子进程验证退避增长，再用一个长驻子进程验证 stop() 不触发 crash。
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from supervisor import Supervisor  # noqa: E402

passed = failed = 0


def check(name, cond, extra=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS {name}")
    else:
        failed += 1
        print(f"  FAIL {name} {extra}")


PY = sys.executable

crashes, delays, restarts = [], [], []
sup = Supervisor(
    [PY, "-c", "import sys; sys.exit(3)"],
    restart_delay_ms=150,
    max_restart_delay_ms=1000,
    max_restarts=2,
    reset_after_ms=10000,
)
sup.on_crash = lambda i: crashes.append(i["code"])
sup.on_restarting = lambda i: delays.append(i["delay"])
sup.on_restarted = lambda i: restarts.append(i["attempt"])
sup.on_give_up = lambda i: crashes.append(("giveup", i["attempts"]))

sup.start()
time.sleep(4.0)
check("crash observed twice", len([c for c in crashes if c == 3]) >= 2, str(crashes))
check("backoff grows", len(delays) >= 2 and delays[1] > delays[0], str(delays))
check("give-up after max_restarts", any(isinstance(c, tuple) and c[0] == "giveup" for c in crashes), str(crashes))
check("not restarting after give-up", not sup.is_running())
sup.stop()

# 长驻子进程：stop() 必须立刻退出且不产生 crash 事件
crashes2 = []
sup2 = Supervisor([PY, "-c", "import time; time.sleep(30)"], restart_delay_ms=100)
sup2.on_crash = lambda i: crashes2.append(i)
p = sup2.start()
check("long child running", sup2.is_running() and p.pid)
time.sleep(0.5)
sup2.stop()
time.sleep(0.8)
check("stop is graceful", not sup2.is_running())
check("stop does not emit crash", crashes2 == [], str(crashes2))
check("status reflects stop", sup2.status()["running"] is False)

print(f"\n结果: {passed} passed, {failed} failed")
sys.exit(0 if failed == 0 else 1)
