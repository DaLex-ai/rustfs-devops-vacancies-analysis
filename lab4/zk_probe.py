"""Real ZooKeeper client tests. Run inside lab4-coordinator image."""
import argparse
import os
import time
import uuid

from kazoo.client import KazooClient

HOSTS = os.getenv("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["probe", "ephemeral"])
args = parser.parse_args()
if args.action == "probe":
    z = KazooClient(hosts=HOSTS, timeout=10)
    try:
        z.start(timeout=7)
        z.ensure_path("/lab4/quorum")
        created = z.create("/lab4/quorum/probe-", b"check", sequence=True)
        value, st = z.get(created)
        assert value == b"check"
        print("CLIENT_WRITE_READ_OK", created, flush=True)
    finally:
        z.stop()
        z.close()
else:
    owner = KazooClient(hosts=HOSTS, timeout=10)
    witness = KazooClient(hosts=HOSTS, timeout=10)
    owner.start(timeout=12)
    witness.start(timeout=12)
    path = "/lab4/ephemeral-" + uuid.uuid4().hex[:10]
    try:
        owner.create(path, b"session-owned", ephemeral=True, makepath=True)
        assert witness.exists(path) is not None
        print("EPHEMERAL_PRESENT", path, flush=True)
        owner.stop()
        owner.close()
        for _ in range(30):
            if witness.exists(path) is None:
                print("EPHEMERAL_DISAPPEARED_AFTER_OWNER_SESSION_CLOSE", path, flush=True)
                break
            time.sleep(0.3)
        else:
            raise AssertionError("Ephemeral znode did not disappear")
    finally:
        try:
            owner.stop()
            owner.close()
        except Exception:
            pass
        witness.stop()
        witness.close()
