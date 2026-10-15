import hashlib
import time

from botocore.exceptions import ClientError
from check_storage import s3

SOURCE_BUCKET = "it-vacancies"
SOURCE_KEY = "vacancies.csv"
TARGET_BUCKET = "hh-lake"
TARGET_KEY = "raw/vacancies.csv"

EXPECTED_SIZE = 847619384
EXPECTED_SHA256 = (
    "303db922b541e4440906f5a7e8be32fb55e8808f950ffddb8c272f1548212a65"
)


def sha256_object(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    digest = hashlib.sha256()
    total = 0

    try:
        for chunk in body.iter_chunks(chunk_size=8 * 1024 * 1024):
            if chunk:
                digest.update(chunk)
                total += len(chunk)
                if total % (128 * 1024 * 1024) < len(chunk):
                    print(f"  Read: {total / 1048576:.0f} MiB", flush=True)
    finally:
        body.close()

    return total, digest.hexdigest()


print("=== VERIFY SOURCE ===", flush=True)

source_info = s3.head_object(
    Bucket=SOURCE_BUCKET,
    Key=SOURCE_KEY,
)

assert source_info["ContentLength"] == EXPECTED_SIZE

source_size, source_hash = sha256_object(
    SOURCE_BUCKET, SOURCE_KEY
)

print("Source SHA256:", source_hash, flush=True)

assert source_size == EXPECTED_SIZE
assert source_hash == EXPECTED_SHA256, "Source checksum mismatch"

print("Source integrity: OK", flush=True)

print("\n=== COPY OBJECT ===", flush=True)

try:
    target_info = s3.head_object(
        Bucket=TARGET_BUCKET,
        Key=TARGET_KEY,
    )
    print("Target already exists; will verify without overwriting.")
except ClientError as e:
    code = e.response["Error"]["Code"]
    if code not in ("404", "NotFound", "NoSuchKey"):
        raise

    started = time.perf_counter()

    s3.copy_object(
        Bucket=TARGET_BUCKET,
        Key=TARGET_KEY,
        CopySource={
            "Bucket": SOURCE_BUCKET,
            "Key": SOURCE_KEY,
        },
    )

    elapsed = time.perf_counter() - started
    print(f"Copy completed in {elapsed:.2f} seconds", flush=True)

print("\n=== VERIFY TARGET ===", flush=True)

target_info = s3.head_object(
    Bucket=TARGET_BUCKET,
    Key=TARGET_KEY,
)

target_size, target_hash = sha256_object(
    TARGET_BUCKET, TARGET_KEY
)

print("Target size:", target_size)
print("Target SHA256:", target_hash)

assert target_info["ContentLength"] == EXPECTED_SIZE
assert target_size == EXPECTED_SIZE
assert target_hash == EXPECTED_SHA256

print("\nALL CHECKS PASSED")
print("Object: s3://hh-lake/raw/vacancies.csv")
