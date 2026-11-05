"""Initialize or inspect an experiment. Never reset an existing run."""
import argparse
import json
import os
import re

from kazoo.client import KazooClient

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["setup", "inspect"])
parser.add_argument("--run", required=True)
args = parser.parse_args()
if not re.fullmatch(r"[a-z][a-z0-9_]{3,70}", args.run):
    raise ValueError("Invalid run id")
root = "/lab4/runs/" + args.run
zk = KazooClient(hosts=os.getenv("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181"), timeout=10)
zk.start(timeout=30)
try:
    if args.action == "setup":
        if zk.exists(root):
            raise RuntimeError(f"Run {args.run} already exists. Use a new run id.")
        zk.ensure_path(root + "/election")
        zk.create(root + "/epoch", b"0")
        zk.create(root + "/pointer", json.dumps({"owner":"none", "epoch":0}).encode())
        print("RUN_INITIALIZED", args.run, flush=True)
    else:
        for item in ("epoch", "pointer"):
            data, stat = zk.get(root + "/" + item)
            print(item.upper(), "VALUE", data.decode(), "VERSION", stat.version, flush=True)
        print("CANDIDATES", zk.get_children(root + "/election"), flush=True)
finally:
    zk.stop()
    zk.close()
