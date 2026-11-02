"""Container adapter for the unchanged HW3 mapper; all objects are run-scoped."""
import argparse
import csv
import hashlib
import io
import json
import os
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from check_storage import s3
from mapreduce_devops import CHUNK_ROWS, TARGET_ROLE, map_chunk

BUCKET = os.environ.get('S3_BUCKET', 'hh-lake')
PREFIX = os.environ['RUN_PREFIX']
PARTS = int(os.environ.get('PARTS', '3'))


def put_json(key, value):
    s3.put_object(Bucket=BUCKET, Key=f'{PREFIX}/{key}',
                  Body=json.dumps(value, ensure_ascii=False).encode())


def get_json(key):
    body = s3.get_object(Bucket=BUCKET, Key=f'{PREFIX}/{key}')['Body']
    try:
        return json.loads(body.read())
    finally:
        body.close()


def memory():
    for name in ('memory.peak', 'memory.max_usage_in_bytes'):
        path = Path('/sys/fs/cgroup') / name
        if path.exists():
            return {'metric': name, 'bytes': int(path.read_text())}
    raise RuntimeError('No cgroup peak counter; do not substitute process RSS')


def rows(key):
    body = s3.get_object(Bucket=BUCKET, Key=key)['Body']
    with io.TextIOWrapper(body, encoding='utf-8-sig', newline='') as stream:
        yield from csv.DictReader(stream)


def record_digest(digest, row):
    digest.update(json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode())
    digest.update(b'\n')


def split():
    """Round-robin whole CSV records; hashes include every field and field order."""
    hashes = [hashlib.sha256() for _ in range(PARTS)]
    counts = [0] * PARTS
    with tempfile.TemporaryDirectory() as directory:
        files = [open(Path(directory) / f'{i}.csv', 'w', encoding='utf-8', newline='')
                 for i in range(PARTS)]
        writers = None
        try:
            for n, row in enumerate(rows('raw/vacancies.csv')):
                if writers is None:
                    writers = [csv.DictWriter(f, fieldnames=list(row)) for f in files]
                    for writer in writers:
                        writer.writeheader()
                i = n % PARTS
                writers[i].writerow(row)
                counts[i] += 1
                record_digest(hashes[i], row)
        finally:
            for f in files:
                f.close()
        if sum(counts) != 229388:
            raise AssertionError(f'Unexpected total record count: {counts}')
        for i in range(PARTS):
            s3.upload_file(str(Path(directory) / f'{i}.csv'), BUCKET,
                           f'{PREFIX}/shards/{i}.csv')
    result = {'parts': PARTS, 'rows': counts, 'total_rows': sum(counts),
              'record_sha256': [h.hexdigest() for h in hashes]}
    put_json('manifest.json', result)
    return result


def calculate(key, index=None):
    begin = time.time()
    counts = Counter()
    chunk = []
    total = selected = skilled = 0
    digest = hashlib.sha256()

    def consume():
        nonlocal selected, skilled
        # Reuse exactly HW3 normalization and combinations, including its 1/3 counters.
        local, n, with_skills = map_chunk(chunk)
        counts.update(local[1])
        selected += n
        skilled += with_skills
        chunk.clear()

    for row in rows(key):
        total += 1
        if index is not None:
            record_digest(digest, row)
        if (row.get('role') or '').strip() == TARGET_ROLE:
            chunk.append(row)
            if len(chunk) == CHUNK_ROWS:
                consume()
    if chunk:
        consume()
    end = time.time()
    result = {'total_rows': total, 'devops_rows': selected, 'with_skills': skilled,
              'unique_pairs': len(counts), 'occurrences': sum(counts.values()),
              'compute_started': begin, 'compute_finished': end,
              'compute_seconds': end - begin, 'record_sha256': digest.hexdigest()}
    if index is not None:
        manifest = get_json('manifest.json')
        if total != manifest['rows'][index] or digest.hexdigest() != manifest['record_sha256'][index]:
            raise AssertionError('Shard record integrity mismatch')
        result['index'] = index
        save_counts(f'partials/{index}.csv', counts)
        put_json(f'partials/{index}.json', result)
    else:
        result.update(verify(counts))
        if (total, selected, skilled) != (229388, 5158, 3534):
            raise AssertionError(result)
        save_counts('single/pairs.csv', counts)
    return result


def read_counts(stream):
    result = Counter()
    for row in csv.DictReader(stream):
        key = (row['skill_1'], row['skill_2'])
        if key in result:
            raise ValueError('Duplicate pair in result')
        count = int(row['count'])
        if count <= 0:
            raise ValueError('Nonpositive pair frequency')
        result[key] = count
    return result


def save_counts(key, counts):
    stream = io.StringIO(newline='')
    writer = csv.writer(stream)
    writer.writerow(['skill_1', 'skill_2', 'count'])
    for pair, count in sorted(counts.items()):
        writer.writerow([*pair, count])
    s3.put_object(Bucket=BUCKET, Key=f'{PREFIX}/{key}', Body=stream.getvalue().encode())


def verify(counts):
    with open('/app/reference/skills_2.csv', encoding='utf-8-sig', newline='') as f:
        reference = read_counts(f)
    if counts != reference:
        raise AssertionError('Full HW3 comparison failed')
    return {'full_match': True, 'missing': 0, 'extra': 0, 'different': 0,
            'top10': [[*pair, n] for pair, n in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]]}


def reduce():
    counts = Counter()
    summaries = []
    for i in range(PARTS):
        summary = get_json(f'partials/{i}.json')
        if summary['index'] != i:
            raise AssertionError('Wrong mapper index')
        summaries.append(summary)
        body = s3.get_object(Bucket=BUCKET, Key=f'{PREFIX}/partials/{i}.csv')['Body']
        with io.TextIOWrapper(body, encoding='utf-8', newline='') as f:
            counts.update(read_counts(f))
    result = {field: sum(s[field] for s in summaries)
              for field in ('total_rows', 'devops_rows', 'with_skills')}
    if tuple(result.values()) != (229388, 5158, 3534):
        raise AssertionError(result)
    overlap = min(s['compute_finished'] for s in summaries) - max(s['compute_started'] for s in summaries)
    if overlap <= 0:
        raise AssertionError('No common overlap of all mapper compute intervals')
    result.update(verify(counts))
    result.update(unique_pairs=len(counts), occurrences=sum(counts.values()),
                  all_mapper_overlap_seconds=overlap, mappers=summaries)
    save_counts('parallel/pairs.csv', counts)
    put_json('comparison.json', result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['single', 'split', 'map', 'reduce'])
    args = parser.parse_args()
    print(json.dumps({'event': 'started', 'mode': args.mode, 'time': time.time()}), flush=True)
    if args.mode == 'single':
        result = calculate('raw/vacancies.csv')
    elif args.mode == 'split':
        result = split()
    elif args.mode == 'map':
        index = int(os.environ['JOB_COMPLETION_INDEX'])
        # Wait for all indexed workers before timing actual data processing.
        put_json(f'ready/{index}.json', {'time': time.time()})
        deadline = time.monotonic() + 120
        while True:
            keys = s3.list_objects_v2(Bucket=BUCKET, Prefix=f'{PREFIX}/ready/').get('Contents', [])
            if len(keys) == PARTS:
                break
            if time.monotonic() > deadline:
                raise TimeoutError('Parallel worker barrier timed out')
            time.sleep(0.2)
        result = calculate(f'{PREFIX}/shards/{index}.csv', index)
    else:
        result = reduce()
    result['memory'] = memory()
    result['mode'] = args.mode
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
