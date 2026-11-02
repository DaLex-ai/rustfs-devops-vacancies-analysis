"""Run isolated, evidenced Kubernetes experiments. No git operations or host data removal."""
import argparse
import base64
import csv
import hashlib
import io
import json
import math
import os
import secrets
import re
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
TOOLS = Path(os.environ.get('K8S_TOOLS', r'C:\rustfs-lab\k8s-tools'))
KUBECTL = str(TOOLS / 'kubectl.exe') if os.name == 'nt' else 'kubectl'
KUBE = ['--kubeconfig', str(TOOLS / 'devops-kubeconfig'), '--context', 'kind-devops-k8s']
NS = 'devops-compute'
IMAGE = 'devops-mapreduce:k8s'
RUN = os.environ.get('LAB_RUN', '20261030-01')
if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,35}', RUN):
    raise ValueError('LAB_RUN must be a short lowercase Kubernetes-name suffix')
OUT = BASE / 'kubernetes' / 'evidence' / RUN
MAN = BASE / 'kubernetes' / 'manifests'
OUT.mkdir(parents=True, exist_ok=True)
MAN.mkdir(parents=True, exist_ok=True)


def save(name, value):
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)
    (OUT / name).write_text(text + '\n', encoding='utf-8')


def command(args, *, data=None, check=True, timeout=180):
    p = subprocess.run(args, input=data, text=True, encoding='utf-8',
                       errors='replace', capture_output=True, timeout=timeout)
    if check and p.returncode:
        # Never print input (may be a Secret).
        raise RuntimeError(f'{args[0]} failed ({p.returncode}): {p.stderr[:3000]}')
    return p


def k(*args, **kwargs):
    return command([KUBECTL, *KUBE, *args], **kwargs)


def obj(*args):
    return json.loads(k(*args, '-o', 'json').stdout)


def apply(resource, filename=None):
    content = json.dumps(resource)
    if filename:
        # JSON is a subset of YAML and is accepted directly by kubectl.
        (MAN / filename).write_text(json.dumps(resource, indent=2) + '\n', encoding='utf-8')
    k('apply', '-f', '-', data=content)


def namespace(name):
    apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name}}, name + '.yaml')


def snapshot(label, ns=NS):
    save(label + '-pods.json', obj('get', 'pods', '-n', ns))
    save(label + '-events.json', obj('get', 'events', '-n', ns))
    save(label + '-describe.txt', k('describe', 'pods', '-n', ns).stdout)


def setup():
    namespace(NS)
    if not k('get', 'secret', 'rustfs-auth', '-n', NS, check=False).returncode:
        print('Existing namespace secret retained')
    else:
        apply({'apiVersion': 'v1', 'kind': 'Secret',
               'metadata': {'name': 'rustfs-auth', 'namespace': NS}, 'type': 'Opaque',
               'stringData': {'RUSTFS_ACCESS_KEY': 'lab-' + secrets.token_hex(8),
                              'RUSTFS_SECRET_KEY': secrets.token_urlsafe(32)}})
    config = {'RUSTFS_VOLUMES': '/data/rustfs{0...3}', 'RUSTFS_ADDRESS': '0.0.0.0:9000',
              'RUSTFS_CONSOLE_ENABLE': 'false', 'RUSTFS_ERASURE_SET_DRIVE_COUNT': '4',
              'RUSTFS_STORAGE_CLASS_STANDARD': 'EC:2', 'RUSTFS_UNSAFE_BYPASS_DISK_CHECK': 'true',
              'RUSTFS_OBS_LOGGER_LEVEL': 'info', 'RUSTFS_OBS_LOG_DIRECTORY': '/app/logs'}
    apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'rustfs-config', 'namespace': NS},
           'data': config}, 'rustfs-config.yaml')
    apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'rustfs', 'namespace': NS},
           'spec': {'selector': {'app': 'rustfs'}, 'ports': [{'name': 's3', 'port': 9000}]}}, 'rustfs-service.yaml')
    mounts = [{'name': f'disk{i}', 'mountPath': f'/data/rustfs{i}'} for i in range(4)]
    mounts.append({'name': 'logs', 'mountPath': '/app/logs'})
    container = {'name': 'rustfs', 'image': 'rustfs/rustfs:1.0.0', 'imagePullPolicy': 'IfNotPresent',
                 'envFrom': [{'secretRef': {'name': 'rustfs-auth'}}, {'configMapRef': {'name': 'rustfs-config'}}],
                 'ports': [{'containerPort': 9000}], 'volumeMounts': mounts,
                 'resources': {'requests': {'cpu': '250m', 'memory': '256Mi'},
                               'limits': {'cpu': '1500m', 'memory': '1Gi'}},
                 'startupProbe': {'httpGet': {'path': '/health', 'port': 9000}, 'periodSeconds': 5, 'failureThreshold': 36},
                 'readinessProbe': {'httpGet': {'path': '/health/ready', 'port': 9000}, 'periodSeconds': 5},
                 'livenessProbe': {'httpGet': {'path': '/health', 'port': 9000}, 'periodSeconds': 10, 'failureThreshold': 6}}
    # Four PVCs demonstrate EC layout; these share one physical host, not four failure domains.
    claims = [{'metadata': {'name': f'disk{i}'}, 'spec': {'accessModes': ['ReadWriteOnce'],
               'storageClassName': 'standard', 'resources': {'requests': {'storage': '3Gi'}}}} for i in range(4)]
    apply({'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'metadata': {'name': 'rustfs', 'namespace': NS},
           'spec': {'serviceName': 'rustfs', 'replicas': 1, 'selector': {'matchLabels': {'app': 'rustfs'}},
                    'template': {'metadata': {'labels': {'app': 'rustfs'}},
                                 'spec': {'securityContext': {'runAsUser': 10001, 'runAsGroup': 10001, 'fsGroup': 10001},
                                          'containers': [container], 'volumes': [{'name': 'logs', 'emptyDir': {}}]}}, 'volumeClaimTemplates': claims}}, 'rustfs-statefulset.yaml')
    k('rollout', 'status', 'statefulset/rustfs', '-n', NS, '--timeout=180s', timeout=200)
    save('nodes.json', obj('get', 'nodes'))
    save('node-describe.txt', k('describe', 'nodes').stdout)
    save('system-pods.json', obj('get', 'pods', '-A'))
    save('versions.json', obj('version'))
    save('pvc.json', obj('get', 'pvc', '-n', NS))
    save('docker-node-limits.json', json.loads(command(['docker', 'inspect', '--format',
         '{{json .HostConfig}}', 'devops-k8s-control-plane']).stdout))
    snapshot('setup')
    print('SETUP PASSED', flush=True)


def cluster_client():
    import boto3
    from botocore.config import Config
    credentials = obj('get', 'secret', 'rustfs-auth', '-n', NS)['data']
    return boto3.client('s3', endpoint_url='http://127.0.0.1:19090', region_name='us-east-1',
                         aws_access_key_id=base64.b64decode(credentials['RUSTFS_ACCESS_KEY']).decode(),
                         aws_secret_access_key=base64.b64decode(credentials['RUSTFS_SECRET_KEY']).decode(),
                         config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))


def forward():
    p = subprocess.Popen([KUBECTL, *KUBE, 'port-forward', '-n', NS, 'service/rustfs', '19090:9000'],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    import socket
    for _ in range(60):
        if p.poll() is not None:
            raise RuntimeError('port-forward exited')
        try:
            with socket.create_connection(('127.0.0.1', 19090), timeout=1):
                return p
        except OSError:
            time.sleep(0.5)
    p.terminate()
    raise TimeoutError('S3 port-forward not ready')


def sha(client, key):
    body = client.get_object(Bucket='hh-lake', Key=key)['Body']
    digest = hashlib.sha256()
    try:
        for part in body.iter_chunks(1024 * 1024):
            digest.update(part)
    finally:
        body.close()
    return digest.hexdigest()


def migrate():
    from check_storage import s3 as source
    from boto3.s3.transfer import TransferConfig
    p = forward()
    try:
        target = cluster_client()
        if 'hh-lake' not in [b['Name'] for b in target.list_buckets()['Buckets']]:
            target.create_bucket(Bucket='hh-lake')
        key = 'raw/vacancies.csv'
        source_info = source.head_object(Bucket='hh-lake', Key=key)
        source_hash = sha(source, key)
        body = source.get_object(Bucket='hh-lake', Key=key)['Body']
        try:
            target.upload_fileobj(body, 'hh-lake', key,
                 Config=TransferConfig(multipart_chunksize=16 * 1024 * 1024, max_concurrency=2))
        finally:
            body.close()
        target_info = target.head_object(Bucket='hh-lake', Key=key)
        target_hash = sha(target, key)
        result = {'source_bytes': source_info['ContentLength'], 'target_bytes': target_info['ContentLength'],
                  'source_sha256': source_hash, 'target_sha256': target_hash,
                  'full_match': source_hash == target_hash and source_info['ContentLength'] == target_info['ContentLength']}
        save('migration.json', result)
        if not result['full_match']:
            raise AssertionError('Migration verification failed')
        print('MIGRATION PASSED', result, flush=True)
    finally:
        p.terminate()
        p.wait(timeout=15)


def job(name, mode, limit='256Mi', request='64Mi', *, prefix=None, indexed=False):
    env = [{'name': 'RUSTFS_ENDPOINT', 'value': 'http://rustfs:9000'},
           {'name': 'RUN_PREFIX', 'value': prefix or f'kubernetes/{RUN}/{name}'}, {'name': 'PARTS', 'value': '3'}]
    container = {'name': 'compute', 'image': IMAGE, 'imagePullPolicy': 'Never', 'args': [mode],
                 'env': env, 'envFrom': [{'secretRef': {'name': 'rustfs-auth'}}],
                 'resources': {'requests': {'cpu': '250m', 'memory': request},
                               'limits': {'cpu': '1', 'memory': limit}},
                 'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}}}
    spec = {'backoffLimit': 0, 'activeDeadlineSeconds': 600,
            'template': {'metadata': {'labels': {'lab-run': RUN}},
                         'spec': {'restartPolicy': 'Never', 'containers': [container]}}}
    if indexed:
        spec.update(completions=3, parallelism=3, completionMode='Indexed')
    return {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name, 'namespace': NS}, 'spec': spec}


def run_job(resource, expected='Succeeded'):
    name = resource['metadata']['name']
    if k('get', 'job', name, '-n', NS, check=False).returncode == 0:
        raise RuntimeError(f'Job {name} already exists; use a new LAB_RUN, never reuse old evidence')
    apply(resource, name + '.yaml')
    deadline = time.monotonic() + 660
    while time.monotonic() < deadline:
        state = obj('get', 'job', name, '-n', NS)
        cond = state.get('status', {}).get('conditions', [])
        if any(c['type'] in ('Complete', 'Failed') and c['status'] == 'True' for c in cond):
            break
        time.sleep(2)
    else:
        snapshot(name)
        raise TimeoutError(name)
    pods = obj('get', 'pods', '-n', NS, '-l', f'job-name={name}')
    save(name + '-job.json', state)
    save(name + '-pods.json', pods)
    save(name + '-events.json', obj('get', 'events', '-n', NS))
    save(name + '-describe.txt', k('describe', 'pods', '-n', NS, '-l', f'job-name={name}').stdout)
    summaries = []
    for pod in pods['items']:
        podname = pod['metadata']['name']
        log = k('logs', podname, '-n', NS, check=False).stdout
        save(podname + '.txt', log)
        termination = pod['status'].get('containerStatuses', [{}])[0].get('state', {}).get('terminated', {})
        if expected == 'OOMKilled':
            if termination.get('reason') != 'OOMKilled':
                raise AssertionError(f'{name}: expected OOMKilled, got {termination}')
        elif pod['status']['phase'] != expected:
            raise AssertionError(f'{name}: {pod["status"]}')
        else:
            summaries.append(json.loads(log.strip().splitlines()[-1]))
    if not pods['items']:
        raise AssertionError('No Pods recorded')
    save(name + '-summary.json', summaries)
    print(name, expected, summaries, flush=True)
    return summaries


def baseline():
    from check_storage import access_key, secret_key, ENDPOINT
    env = os.environ.copy()
    docker_endpoint = os.environ.get('DOCKER_S3_ENDPOINT', ENDPOINT.replace('://localhost:', '://host.docker.internal:').replace('://127.0.0.1:', '://host.docker.internal:'))
    env.update(RUSTFS_ACCESS_KEY=access_key, RUSTFS_SECRET_KEY=secret_key,
               RUSTFS_ENDPOINT=docker_endpoint, RUN_PREFIX=f'kubernetes/{RUN}/docker')
    p = subprocess.run(['docker', 'run', '--rm', '--memory', '256m', '--cpus', '1',
          *[x for name in ('RUSTFS_ACCESS_KEY', 'RUSTFS_SECRET_KEY', 'RUSTFS_ENDPOINT', 'RUN_PREFIX') for x in ('-e', name)],
          IMAGE, 'single'], env=env, capture_output=True, text=True, encoding='utf-8', timeout=240)
    save('docker-baseline.txt', p.stdout + p.stderr)
    if p.returncode:
        raise RuntimeError('Docker baseline failed; see evidence')
    summary = json.loads(p.stdout.strip().splitlines()[-1])
    if not summary['full_match']:
        raise AssertionError(summary)
    save('docker-baseline.json', summary)
    print('DOCKER BASELINE PASSED', summary, flush=True)


def compute():
    measured = run_job(job('measure-' + RUN, 'single', '256Mi'))[0]
    peak = measured['memory']['bytes'] / 1048576
    low = max(16, int(peak / 2))
    run_job(job('oom-' + RUN, 'single', f'{low}Mi', '16Mi'), expected='OOMKilled')
    sufficient = math.ceil(peak * 1.5 / 16) * 16
    success = run_job(job('safe-' + RUN, 'single', f'{sufficient}Mi'))[0]
    save('memory-comparison.json', {'initial_limit_mib': 256, 'measured_peak_mib': peak,
         'oom_limit_mib': low, 'chosen_limit_mib': sufficient, 'headroom_factor': 1.5,
         'safe_peak_mib': success['memory']['bytes'] / 1048576,
         'historical_windows_peak_mib': 87.97,
         'comparison_note': 'HW3 global counts retain singles+pairs+triples; adapter retains global pairs and temporary original mapper counters. Different workloads and metrics.'})
    prefix = f'kubernetes/{RUN}/parallel'
    run_job(job('split-' + RUN, 'split', '1Gi', '128Mi', prefix=prefix))
    run_job(job('map-' + RUN, 'map', '256Mi', prefix=prefix, indexed=True))
    reduced = run_job(job('reduce-' + RUN, 'reduce', '256Mi', prefix=prefix))[0]
    save('comparison.json', reduced)
    p = forward()
    try:
        client = cluster_client()
        client.download_file('hh-lake', prefix + '/parallel/pairs.csv', str(OUT / 'pairs.csv'))
        save('partition-manifest.json', json.loads(client.get_object(Bucket='hh-lake', Key=prefix + '/manifest.json')['Body'].read()))
    finally:
        p.terminate()
        p.wait(timeout=15)


def persistence():
    old = obj('get', 'pod', 'rustfs-0', '-n', NS)['metadata']['uid']
    k('delete', 'pod', 'rustfs-0', '-n', NS, '--wait=true')
    k('rollout', 'status', 'statefulset/rustfs', '-n', NS, '--timeout=180s', timeout=200)
    new = obj('get', 'pod', 'rustfs-0', '-n', NS)['metadata']['uid']
    p = forward()
    try:
        digest = sha(cluster_client(), 'raw/vacancies.csv')
        migration = json.loads((OUT / 'migration.json').read_text(encoding='utf-8'))
        result = {'old_pod_uid': old, 'new_pod_uid': new, 'sha256_after_restart': digest,
                  'passed': old != new and digest == migration['source_sha256']}
        save('persistence.json', result)
        if not result['passed']:
            raise AssertionError(result)
        print('PERSISTENCE PASSED', flush=True)
    finally:
        p.terminate()
        p.wait(timeout=15)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['setup', 'baseline', 'migrate', 'compute', 'persistence'])
    args = parser.parse_args()
    globals()[args.stage]()
