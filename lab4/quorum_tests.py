"""Three-node quorum and ephemeral-node demonstration. Logs real command output."""
import json
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "evidence" / "ensemble"
OUT.mkdir(parents=True, exist_ok=True)


def run(*args, timeout=50):
    return subprocess.run(args, cwd=ROOT, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", timeout=timeout)


def docker_compose(*args, timeout=50):
    return run("docker", "compose", "--profile", "workers", *args, timeout=timeout)


def fourlw(port, command="srvr"):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.sendall((command + "\n").encode())
        sock.settimeout(4)
        data = []
        while True:
            try:
                b = sock.recv(65536)
            except socket.timeout:
                break
            if not b:
                break
            data.append(b)
    return b"".join(data).decode(errors="replace")


def roles():
    found = {}
    for num in (1, 2, 3):
        try:
            info = fourlw(2180 + num)
            role = next((line.rsplit(":", 1)[-1].strip().lower()
                         for line in info.splitlines() if line.startswith("Mode:")), "unknown")
        except OSError as exc:
            role = f"unreachable: {exc}"
        found[num] = role
    return found


def probe(expect_ok=True):
    proc = docker_compose("run", "--rm", "--no-deps", "worker-a", "zk_probe.py", "probe", timeout=25)
    if expect_ok and (proc.returncode or "CLIENT_WRITE_READ_OK" not in proc.stdout):
        raise AssertionError(f"Expected client success but got {proc.returncode}: {proc.stdout} {proc.stderr}")
    if not expect_ok and not proc.returncode:
        raise AssertionError("Single ZooKeeper server unexpectedly accepted consistent client write")
    return {"returncode": proc.returncode, "stdout": proc.stdout[-1600:], "stderr": proc.stderr[-1600:]}


def main():
    logs = []
    stopped = []
    try:
        all_up = docker_compose("up", "-d", "zk1", "zk2", "zk3", timeout=90)
        if all_up.returncode:
            raise RuntimeError(all_up.stderr)
        for _ in range(45):
            original = roles()
            if sorted(original.values()) == ["follower", "follower", "leader"]:
                break
            time.sleep(1)
        else:
            raise TimeoutError(f"Three-node roles not ready: {original}")
        logs.append({"utc": datetime.now(timezone.utc).isoformat(), "step": "all_three", "roles": original, "probe": probe()})
        followers = [node for node, role in original.items() if role == "follower"]
        first, second = followers
        proc = docker_compose("stop", f"zk{first}")
        if proc.returncode:
            raise RuntimeError(proc.stderr)
        stopped.append(first)
        time.sleep(5)
        logs.append({"step": "one_follower_down", "stopped": first, "roles": roles(), "probe": probe()})
        proc = docker_compose("stop", f"zk{second}")
        if proc.returncode:
            raise RuntimeError(proc.stderr)
        stopped.append(second)
        time.sleep(8)
        logs.append({"step": "two_followers_down", "stopped": stopped[:], "roles": roles(), "probe": probe(expect_ok=False)})
    finally:
        for node in stopped:
            docker_compose("up", "-d", f"zk{node}", timeout=70)
        for attempt in range(50):
            restored = roles()
            if sorted(restored.values()) == ["follower", "follower", "leader"]:
                break
            time.sleep(1)
        logs.append({"step": "restored", "roles": restored})
        (OUT / "quorum.json").write_text(json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(logs, ensure_ascii=False, indent=2), flush=True)
    if sorted(roles().values()) != ["follower", "follower", "leader"]:
        raise AssertionError("Ensemble was not restored")
    ephemeral = docker_compose("run", "--rm", "--no-deps", "worker-a", "zk_probe.py", "ephemeral", timeout=60)
    text = ephemeral.stdout + "\n" + ephemeral.stderr
    (OUT / "ephemeral.txt").write_text(text, encoding="utf-8")
    if ephemeral.returncode or "EPHEMERAL_DISAPPEARED_AFTER_OWNER_SESSION_CLOSE" not in text:
        raise AssertionError("Ephemeral verification failed: " + text)
    print("ENSEMBLE_AND_EPHEMERAL_PASSED", flush=True)


if __name__ == "__main__":
    main()
