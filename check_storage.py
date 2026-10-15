import json
import os
import subprocess

import boto3
from botocore.config import Config

ENDPOINT = os.getenv(
    "RUSTFS_ENDPOINT",
    "http://localhost:9000"
)


def get_credentials():
    access = os.getenv("RUSTFS_ACCESS_KEY")
    secret = os.getenv("RUSTFS_SECRET_KEY")

    if access and secret:
        return access, secret

    if access or secret:
        raise RuntimeError(
            "Both RustFS environment variables are required"
        )

    try:
        output = subprocess.check_output(
            ["docker", "inspect", "rustfs-server"],
            text=True,
            encoding="utf-8",
            stderr=subprocess.DEVNULL
        )

        container = json.loads(output)[0]

        environment = dict(
            item.split("=", 1)
            for item in container["Config"]["Env"]
            if "=" in item
        )

        access = environment.get("RUSTFS_ACCESS_KEY")
        secret = environment.get("RUSTFS_SECRET_KEY")

        if access and secret:
            return access, secret

    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        ValueError,
        KeyError,
        IndexError
    ):
        pass

    raise RuntimeError(
        "RustFS credentials unavailable. "
        "Set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY "
        "in your local PowerShell session."
    )


access_key, secret_key = get_credentials()

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}
    )
)


def main():
    print("=== RUSTFS CONNECTION CHECK ===")

    buckets = [
        bucket["Name"]
        for bucket in s3.list_buckets()["Buckets"]
    ]

    print("Connection: OK")
    print("Buckets:", buckets)

    if "hh-lake" not in buckets:
        raise RuntimeError("Bucket hh-lake not found")

    info = s3.head_object(
        Bucket="hh-lake",
        Key="raw/vacancies.csv"
    )

    assert info["ContentLength"] == 847619384

    print("Dataset size:", info["ContentLength"])
    print("Storage verification: SUCCESS")


if __name__ == "__main__":
    main()
