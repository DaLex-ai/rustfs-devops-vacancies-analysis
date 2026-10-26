"""ZooKeeper lab 4: real pair counting, resumable checkpoints, stale-writer demonstration."""
import argparse
import csv
import fcntl
import hashlib
import itertools
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import BadVersionError, ConnectionLoss, SessionExpiredError

ROOT = Path("/app")
RUN = os.environ["LAB4_RUN"]
FENCING = os.environ.get("FENCING", "0") == "1"
HOSTS = os.environ.get("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
DELAY = float(os.environ.get("WORK_DELAY", "0.45"))
PATH = f"/lab4/runs/{RUN}"
LOGDIR = ROOT / "logs" / RUN
DATADIR = ROOT / "results" / RUN
ROWS_PATH = Path("/inputs/devops_skills.csv")
REFERENCE_PATH = Path("/inputs/skills_2.csv")
CHUNK = 250
EXPECTED = (22311, 134786)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def log(worker, event, **fields):
    LOGDIR.mkdir(parents=True, exist_ok=True)
    payload = {"utc": now(), "worker": worker, "event": event, **fields}
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    print(text, flush=True)
    with (LOGDIR / f"{worker}.txt").open("a", encoding="utf-8") as fh:
        fh.write(text + "\n")


def load_chunks():
    with ROWS_PATH.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if len(rows) != 5158:
        raise AssertionError(f"Curated rows != 5158: {len(rows)}")
    chunks = [rows[i:i + CHUNK] for i in range(0, len(rows), CHUNK)]
    assert len(chunks) == 21
    return chunks


def count_chunk(chunk):
    counts = Counter()
    for row in chunk:
        skills = sorted({s.strip() for s in (row.get("key_skills") or "").split(";") if s.strip()})
        counts.update(itertools.combinations(skills, 2))
    return counts


def read_checkpoint():
    path = DATADIR / "checkpoint.json"
    if not path.exists():
        return 0, Counter()
    payload = json.loads(path.read_text(encoding="utf-8"))
    counts = Counter({(a, b): int(c) for a, b, c in payload["pairs"]})
    return int(payload["next_chunk"]), counts


def save_checkpoint(index, counts, worker):
    DATADIR.mkdir(parents=True, exist_ok=True)
    lock_path = DATADIR / "checkpoint.lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            previous, _ = read_checkpoint()
            if index <= previous:
                return
            payload = {
                "next_chunk": index,
                "saved_by": worker,
                "utc": now(),
                "pairs": [[a, b, n] for (a, b), n in sorted(counts.items())],
            }
            temporary = DATADIR / f"checkpoint.{worker}.tmp"
            temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            os.replace(temporary, DATADIR / "checkpoint.json")
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def verify_and_save(counts, worker):
    actual = (len(counts), sum(counts.values()))
    if actual != EXPECTED:
        raise AssertionError(f"Wrong pair totals: {actual} != {EXPECTED}")
    reference = Counter()
    with REFERENCE_PATH.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            key = row["skill_1"], row["skill_2"]
            if key in reference:
                raise AssertionError("Repeated reference pair")
            reference[key] = int(row["count"])
    if reference != counts:
        missing = len(reference.keys() - counts.keys())
        extra = len(counts.keys() - reference.keys())
        different = sum(reference[k] != counts[k] for k in reference.keys() & counts.keys())
        raise AssertionError(f"Not exact: missing={missing} extra={extra} different={different}")
    DATADIR.mkdir(parents=True, exist_ok=True)
    path = DATADIR / f"pairs_{worker}.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["skill_1", "skill_2", "count"])
        for (a, b), freq in sorted(counts.items()):
            writer.writerow([a, b, freq])
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    log(worker, "EXACT_MATCH", unique=actual[0], occurrences=actual[1], file=str(path), sha256=sha)
    return sha


def wait_leader(zk, candidate, worker):
    while True:
        children = sorted(zk.get_children(PATH + "/election"))
        if not children:
            raise RuntimeError("No election candidates")
        if candidate in children and candidate == children[0]:
            for attempt in range(10):
                raw, st = zk.get(PATH + "/epoch")
                value = int(raw.decode()) + 1
                try:
                    zk.set(PATH + "/epoch", str(value).encode(), version=st.version)
                    log(worker, "ROLE_LEADER", epoch=value, candidate=candidate)
                    return value
                except BadVersionError:
                    time.sleep(0.2)
            raise RuntimeError("Could not assign epoch")
        if candidate not in children:
            raise RuntimeError("Own election znode vanished before leadership")
        time.sleep(0.25)


def main(worker):
    log(worker, "START", fencing=int(FENCING), run=RUN, session_timeout_sec=10)
    chunks = load_chunks()
    zk = KazooClient(hosts=HOSTS, timeout=10.0)
    def state_changed(state):
        log(worker, "SESSION_STATE", state=str(state))
    zk.add_listener(state_changed)
    zk.start(timeout=30)
    if not zk.exists(PATH + "/pointer"):
        raise RuntimeError("Run not initialized. Execute setup_run.py")
    pointer_raw, initial_stat = zk.get(PATH + "/pointer")
    candidate_path = zk.create(PATH + "/election/candidate-", worker.encode(), ephemeral=True, sequence=True)
    candidate = candidate_path.rsplit("/", 1)[-1]
    log(worker, "ELECTION_JOIN", candidate=candidate, initial_pointer_version=initial_stat.version)
    try:
        epoch = wait_leader(zk, candidate, worker)
        # Intentionally cache the pre-failover pointer version to demonstrate stale writers.
        prewrite_version = initial_stat.version
        start, counts = read_checkpoint()
        log(worker, "RESUMED_FROM_CHECKPOINT", next_chunk=start, existing_pairs=len(counts))
        for i in range(start, len(chunks)):
            counts.update(count_chunk(chunks[i]))
            save_checkpoint(i + 1, counts, worker)
            log(worker, "CHUNK_DONE", chunk=i + 1, total_chunks=len(chunks))
            time.sleep(DELAY)
        digest = verify_and_save(counts, worker)
        payload = json.dumps({
            "owner": worker, "epoch": epoch, "sha256": digest,
            "unique_pairs": len(counts), "occurrences": sum(counts.values()), "utc": now()
        }, ensure_ascii=False).encode("utf-8")
        # Stale process may have lost its ephemeral node while paused.
        # In unprotected mode write unconditionally; in fenced mode the *receiver*
        # (the versioned pointer in ZooKeeper) rejects stale version.
        if FENCING:
            try:
                stat = zk.set(PATH + "/pointer", payload, version=prewrite_version)
                log(worker, "PUBLISHED_RESULT", pointer_version=stat.version, epoch=epoch)
            except BadVersionError as exc:
                log(worker, "BadVersionError", reason="STALE_EPOCH_FENCED", epoch=epoch,
                    expected_pointer_version=prewrite_version, details=str(exc))
        else:
            stat = zk.set(PATH + "/pointer", payload, version=-1)
            log(worker, "PUBLISHED_RESULT", pointer_version=stat.version, epoch=epoch)
        log(worker, "FINISHED")
    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=["a", "b"], required=True)
    args = parser.parse_args()
    try:
        main(args.worker)
    except Exception as exc:
        log(args.worker, "FATAL", type=type(exc).__name__, message=str(exc))
        raise
