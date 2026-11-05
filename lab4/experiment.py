"""Repeatable failover exercise. Runs only lab4 containers, never RustFS."""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def dc(*args, env=None, check=True):
    proc = subprocess.run(["docker", "compose", "--profile", "workers", *args],
                          cwd=HERE, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and proc.returncode:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {proc.stdout[-2500:]} {proc.stderr[-2500:]}")
    return proc.stdout.strip()


def docker(*args):
    proc = subprocess.run(["docker", *args], capture_output=True, text=True,
                          encoding="utf-8", errors="replace")
    if proc.returncode:
        raise RuntimeError(f"docker {' '.join(args)}: {proc.stderr[-2500:]}")
    return proc.stdout.strip()


def events(run, worker):
    log = HERE / "logs" / run / (worker + ".txt")
    if not log.exists():
        return []
    result = []
    for line in log.read_text(encoding="utf-8").splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return result


def first_event(run, worker, event, **criteria):
    return next((x for x in events(run, worker)
                 if x.get("event") == event and all(x.get(k) == v for k, v in criteria.items())), None)


def until(predicate, timeout, label):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.45)
    raise TimeoutError(f"Timed out: {label}")


def main(mode):
    fencing = int(mode == "fencing")
    run = f"{mode.replace('-', '_')}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    env = dict(os.environ, LAB4_RUN=run, FENCING=str(fencing), WORK_DELAY="0.55")
    directory = HERE / "evidence" / run
    directory.mkdir(parents=True, exist_ok=False)
    timeline = [{"utc": stamp(), "event": "EXPERIMENT_START", "mode": mode, "run": run}]
    print("RUN_ID", run, flush=True)
    dc("up", "-d", "zk1", "zk2", "zk3")
    try:
        dc("stop", "worker-a", "worker-b", check=False)
        dc("rm", "-f", "worker-a", "worker-b", check=False)
        print(dc("run", "--rm", "--no-deps", "worker-a", "zk_state.py", "setup", "--run", run, env=env), flush=True)
        dc("up", "-d", "--no-deps", "worker-a", env=env)
        until(lambda: first_event(run, "a", "ROLE_LEADER"), 45, "worker A becomes leader")
        dc("up", "-d", "--no-deps", "worker-b", env=env)
        until(lambda: first_event(run, "b", "ELECTION_JOIN"), 45, "worker B joins election")
        until(lambda: first_event(run, "a", "CHUNK_DONE", chunk=5), 60, "five real chunks processed")
        # Pause before worker A can complete, after its fifth persisted checkpoint.
        pause_start = stamp()
        pause_monotonic = time.monotonic()
        docker("pause", "lab4-coordinator-a")
        timeline.append({"utc": pause_start, "event": "PAUSE_A"})
        print("PAUSED_A", pause_start, flush=True)
        takeover = until(lambda: first_event(run, "b", "ROLE_LEADER"), 35, "standby B elected")
        timeline.append({"utc": takeover["utc"], "event": "TAKEOVER_B", "epoch": takeover["epoch"]})
        from datetime import datetime as dt
        takeover_observed = stamp()
        change = time.monotonic() - pause_monotonic
        timeline.append({"utc": takeover_observed, "event": "TAKEOVER_OBSERVED_ON_HOST", "source_container_utc": takeover["utc"]})
        print("FAILOVER_SECONDS_HOST_MONOTONIC", round(change, 3), flush=True)
        resumed = until(lambda: first_event(run, "b", "RESUMED_FROM_CHECKPOINT"), 10, "B resumes checkpoint")
        if resumed["next_chunk"] < 5:
            raise AssertionError(f"Standby did not resume checkpoint: {resumed}")
        print("RESUMED_FROM_CHUNK", resumed["next_chunk"], flush=True)
        until(lambda: first_event(run, "b", "PUBLISHED_RESULT"), 100, "B publishes exact result")
        # Preserve the mandatory approximate 40-second pause.
        elapsed = (dt.now(timezone.utc) - dt.fromisoformat(pause_start)).total_seconds()
        time.sleep(max(0, 40 - elapsed))
        unpause_at = stamp()
        docker("unpause", "lab4-coordinator-a")
        timeline.append({"utc": unpause_at, "event": "UNPAUSE_A"})
        print("UNPAUSED_A", unpause_at, flush=True)
        a_end = until(lambda: first_event(run, "a", "BadVersionError" if fencing else "PUBLISHED_RESULT")
                      or first_event(run, "a", "FATAL"), 110, "old leader finishes or is fenced")
        if a_end["event"] == "FATAL":
            raise AssertionError(f"Old leader crashed: {a_end}")
        until(lambda: first_event(run, "a", "FINISHED"), 10, "old leader finish")
        state = dc("run", "--rm", "--no-deps", "worker-a",
                   "zk_state.py", "inspect", "--run", run, env=env)
        print("FINAL_STATE", state, flush=True)
        version = 1 if fencing else 2
        if f"VERSION {version}" not in state:
            raise AssertionError(f"Expected pointer version {version}: {state}")
        if '"owner": "b"' not in state and fencing:
            raise AssertionError("Fenced pointer must remain with standby B")
        for worker in ("a", "b"):
            if not first_event(run, worker, "EXACT_MATCH"):
                raise AssertionError(f"Worker {worker}: no exact result verification")
        successful = sum(bool(first_event(run, worker, "PUBLISHED_RESULT")) for worker in ("a", "b"))
        if successful != (1 if fencing else 2):
            raise AssertionError(f"Wrong successful publish count: {successful}")
        summary = {"run": run, "fencing": fencing, "failover_seconds": change,
                   "failover_clock": "Windows host monotonic seconds (includes poll latency <= 0.45 s)",
                   "standby_checkpoint_chunk": resumed["next_chunk"],
                   "successful_publications": successful, "expected_version": version,
                   "pointer": state, "timeline": timeline, "status": "PASSED"}
        (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print("EXPERIMENT_PASSED", mode, "evidence", directory, flush=True)
    except BaseException as exc:
        (directory / "error.txt").write_text(f"{stamp()} {type(exc).__name__}: {exc}\n", encoding="utf-8")
        raise
    finally:
        # Only workers. Leave ZooKeeper ensemble available for quorum demonstrations.
        try:
            docker("unpause", "lab4-coordinator-a")
        except Exception:
            pass
        dc("stop", "worker-a", "worker-b", check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["no-fencing", "fencing"])
    args = parser.parse_args()
    main(args.mode)
