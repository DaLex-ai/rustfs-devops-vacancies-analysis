"""Independent evidence audit; compares saved raw Pod states and CSV results."""
import csv
import hashlib
import json
from pathlib import Path
from run import BASE, OUT, RUN, NS, k, obj, save


def load(name):
    return json.loads((OUT / name).read_text(encoding='utf-8'))


def counts(path):
    result = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            pair = (row['skill_1'], row['skill_2'])
            if pair in result:
                raise AssertionError('Duplicate pair')
            result[pair] = int(row['count'])
    return result


def main():
    checks = {}
    migration = load('migration.json')
    checks['migration_hash_and_size'] = (migration['source_sha256'] == migration['target_sha256']
          and migration['source_bytes'] == migration['target_bytes'] == 847619384)
    checks['pvc_survives_pod_replacement'] = load('persistence.json')['passed']
    for phase in ('measure', 'safe', 'split', 'map', 'reduce'):
        pods = load(f'{phase}-{RUN}-pods.json')['items']
        checks[phase + '_pods_completed'] = bool(pods) and all(
            p['status']['phase'] == 'Succeeded' and all(c['state']['terminated']['exitCode'] == 0
            and c['restartCount'] == 0 for c in p['status']['containerStatuses']) for p in pods)
    oom = load(f'oom-{RUN}-pods.json')['items']
    checks['oom_confirmed_without_retry'] = len(oom) == 1 and all(
        c['state']['terminated']['reason'] == 'OOMKilled' and c['state']['terminated']['exitCode'] == 137
        and c['restartCount'] == 0 for c in oom[0]['status']['containerStatuses'])
    actual = counts(OUT / 'pairs.csv')
    reference = counts(BASE / 'results/mapreduce/skills_2.csv')
    duckdb = counts(BASE / 'results/duckdb/skills_2.csv')
    checks['all_pairs_exact_three_way_match'] = actual == reference == duckdb
    checks['pair_totals'] = len(actual) == 22311 and sum(actual.values()) == 134786
    top10 = [[*pair, n] for pair, n in sorted(actual.items(), key=lambda x: (-x[1], x[0]))[:10]]
    comparison = load('comparison.json')
    checks['top10_exact_match'] = top10 == comparison['top10']
    shards = load('partition-manifest.json')
    checks['all_records_covered'] = shards['total_rows'] == sum(shards['rows']) == 229388
    checks['each_shard_integrity'] = all(m['record_sha256'] == shards['record_sha256'][m['index']]
           and m['total_rows'] == shards['rows'][m['index']] for m in comparison['mappers'])
    mapper_pods = load(f'map-{RUN}-pods.json')['items']
    indexes = {p['metadata']['annotations']['batch.kubernetes.io/job-completion-index'] for p in mapper_pods}
    checks['three_distinct_indexed_pods'] = len(mapper_pods) == 3 and indexes == {'0', '1', '2'}
    checks['real_parallel_compute'] = comparison['all_mapper_overlap_seconds'] > 0
    checks['cpu_wait_then_completion'] = load('cpu-result.json')['same_pod_completed']
    checks['scheduler_preempted_event'] = any(e['reason'] == 'Preempted' for e in load('priority-result.json')['preemption_events'])
    checks['quota_admission_reject_and_recovery'] = all(load('quota-result.json').values())
    pods = obj('get', 'pods', '-A')
    checks['system_pods_healthy'] = all(p['status']['phase'] == 'Running' and
        all(c.get('ready', False) for c in p['status'].get('containerStatuses', []))
        for p in pods['items'] if p['metadata']['namespace'] == 'kube-system')
    rustfs = obj('get', 'pod', 'rustfs-0', '-n', NS)
    checks['rustfs_ready'] = all(c['ready'] for c in rustfs['status']['containerStatuses'])
    node = obj('get', 'nodes')['items'][0]
    checks['node_ready_no_pressure'] = all(c['status'] == ('True' if c['type'] == 'Ready' else 'False')
        for c in node['status']['conditions'] if c['type'] in ('Ready', 'MemoryPressure', 'DiskPressure', 'PIDPressure'))
    # Check actual credentials without ever displaying or writing the values.
    import base64
    from check_storage import access_key, secret_key
    sensitive = [access_key.encode(), secret_key.encode()]
    secret = obj('get', 'secret', 'rustfs-auth', '-n', NS)['data']
    sensitive += [base64.b64decode(v) for v in secret.values()]
    exposed = []
    # Write final snapshots before computing their hashes.
    save('final-pods.json', pods)
    save('final-node.json', node)
    hashes = {}
    for path in (BASE / 'kubernetes').rglob('*'):
        if path.is_file() and '__pycache__' not in path.parts and path.suffix not in ('.pdf', '.pyc'):
            body = path.read_bytes()
            if any(value in body for value in sensitive):
                exposed.append(str(path.relative_to(BASE)))
            if path.parent == OUT and path.name not in ('audit.json', 'artifact-hashes.json'):
                hashes[path.name] = hashlib.sha256(body).hexdigest()
    checks['no_credentials_in_new_text_files'] = not exposed
    save('artifact-hashes.json', hashes)
    save('audit.json', {'checks': checks, 'passed': all(checks.values()),
                        'failed_checks': [name for name, passed in checks.items() if not passed],
                        'exposed_files': exposed, 'top10': top10,
                        'rubric_available': False,
                        'note': 'Checks cover approved specification, not an unavailable original grading rubric.'})
    print(json.dumps(checks, indent=2), flush=True)
    if not all(checks.values()):
        raise SystemExit('AUDIT FAILED')
    print('AUDIT PASSED', flush=True)


if __name__ == '__main__':
    main()
