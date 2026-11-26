package ipc;

import java.util.ArrayList;
import java.util.List;

/** P2 回归：Java 宿主侧进程守护（退避重启 / 优雅停止）。 */
public class SupervisorTest {

    static int passed = 0, failed = 0;

    static void check(String name, boolean cond, String extra) {
        if (cond) { passed++; System.out.println("  PASS " + name); }
        else { failed++; System.out.println("  FAIL " + name + " " + extra); }
    }

    public static void main(String[] args) throws Exception {
        List<Integer> crashes = new ArrayList<>();
        List<Integer> delays = new ArrayList<>();
        List<Integer> restarts = new ArrayList<>();
        boolean[] gaveUp = {false};

        Supervisor sup = new Supervisor(List.of("python", "-c", "import sys; sys.exit(3)"),
                null, null, true, 150, 1000, 2, 10000, 5000);
        sup.onCrash = e -> crashes.add(e.exitCode);
        sup.onRestarting = e -> delays.add(e.delayMs);
        sup.onRestarted = e -> restarts.add(e.attempt);
        sup.onGiveUp = e -> gaveUp[0] = true;
        sup.start();
        Thread.sleep(4000);

        int c3 = 0;
        for (int c : crashes) if (c == 3) c3++;
        check("crash observed twice", c3 >= 2, crashes.toString());
        check("backoff grows", delays.size() >= 2 && delays.get(1) > delays.get(0), delays.toString());
        check("restarted events", !restarts.isEmpty(), restarts.toString());
        check("give-up after max_restarts", gaveUp[0], "");
        check("not running after give-up", !sup.isRunning(), "");
        sup.stop();

        List<Integer> crashes2 = new ArrayList<>();
        Supervisor sup2 = new Supervisor(List.of("python", "-c", "import time; time.sleep(30)"),
                null, null, true, 100, 30000, null, 30000, 5000);
        sup2.onCrash = e -> crashes2.add(e.exitCode);
        sup2.start();
        check("long child running", sup2.isRunning() && sup2.pid() != null, "");
        Thread.sleep(500);
        sup2.stop();
        Thread.sleep(600);
        check("stop is graceful", !sup2.isRunning(), "");
        check("stop does not emit crash", crashes2.isEmpty(), crashes2.toString());

        System.out.println("\n结果: " + passed + " passed, " + failed + " failed");
        System.exit(failed == 0 ? 0 : 1);
    }
}
