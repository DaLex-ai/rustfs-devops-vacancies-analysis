"""CPU admission, scheduler preemption and namespace quotas: isolated test Pods only."""
import json
import time
from run import IMAGE, RUN, apply, k, namespace, obj, save, snapshot

NS = 'devops-scheduling'


def cpu(value):
    return int(float(value[:-1])) if value.endswith('m') else int(float(value) * 1000)


def available_cpu():
    node = obj('get', 'nodes')['items'][0]
    capacity = cpu(node['status']['allocatable']['cpu'])
    used = 0
    for pod in obj('get', 'pods', '-A')['items']:
        if pod['spec'].get('nodeName') != node['metadata']['name'] or pod['status']['phase'] in ('Succeeded', 'Failed'):
            continue
        regular = sum(cpu(c.get('resources', {}).get('requests', {}).get('cpu', '0')) for c in pod['spec']['containers'])
        init = max([cpu(c.get('resources', {}).get('requests', {}).get('cpu', '0')) for c in pod['spec'].get('initContainers', [])] or [0])
        used += max(regular, init)
    save('scheduler-capacity.json', {'allocatable_millicpu': capacity, 'reserved_millicpu': used,
                                    'free_requests_millicpu': capacity - used,
                                    'note': 'Scheduler capacity and outer Docker 4-CPU execution quota are different quantities.'})
    return capacity - used


def pod(name, request, *, priority=None, work=False, ns=NS, mem='32Mi', limitmem='64Mi'):
    code = 'print(sum(i*i for i in range(1000000)), flush=True)' if work else 'import time; time.sleep(300)'
    spec = {'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 3,
            'containers': [{'name': 'task', 'image': IMAGE, 'imagePullPolicy': 'Never',
                            'command': ['python', '-c', code],
                            'resources': {'requests': {'cpu': request, 'memory': mem},
                                          'limits': {'cpu': request, 'memory': limitmem}}}]}
    if priority:
        spec['priorityClassName'] = priority
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': ns, 'labels': {'lab-run': RUN}}, 'spec': spec}


def wait_phase(name, phase, ns=NS, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        p = obj('get', 'pod', name, '-n', ns)
        if p['status']['phase'] == phase:
            return p
        if p['status']['phase'] == 'Failed':
            raise RuntimeError(p['status'])
        time.sleep(0.5)
    raise TimeoutError(f'{name} not {phase}')


def cpu_pending():
    namespace(NS)
    available = available_cpu()
    if available < 1500:
        raise RuntimeError('Insufficient spare CPU requests for safe demo')
    blocker = 'cpu-blocker-' + RUN
    waiter = 'cpu-waiter-' + RUN
    apply(pod(blocker, f'{available - 500}m'), 'cpu-blocker.yaml')
    wait_phase(blocker, 'Running')
    apply(pod(waiter, '1000m', work=True), 'cpu-waiter.yaml')
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        events = obj('get', 'events', '-n', NS)
        matching = [e for e in events['items'] if e.get('involvedObject', {}).get('name') == waiter
                    and 'Insufficient cpu' in e.get('message', '')]
        if matching:
            break
        time.sleep(1)
    else:
        raise AssertionError('No Insufficient cpu evidence')
    pending = obj('get', 'pod', waiter, '-n', NS)
    if pending['status']['phase'] != 'Pending':
        raise AssertionError('Waiter did not stay Pending')
    snapshot('cpu-pending', NS)
    save('cpu-node-reservations.txt', k('describe', 'nodes').stdout)
    k('delete', 'pod', blocker, '-n', NS, '--wait=true')
    final = wait_phase(waiter, 'Succeeded')
    snapshot('cpu-after-release', NS)
    save('cpu-result.json', {'pending_uid': pending['metadata']['uid'], 'completed_uid': final['metadata']['uid'],
                           'insufficient_cpu': True, 'same_pod_completed': pending['metadata']['uid'] == final['metadata']['uid']})
    print('CPU PENDING / RELEASE PASSED', flush=True)


def preemption():
    for name, priority in [('devops-low', -100), ('devops-high', -50)]:
        apply({'apiVersion': 'scheduling.k8s.io/v1', 'kind': 'PriorityClass',
               'metadata': {'name': name}, 'value': priority, 'globalDefault': False,
               'preemptionPolicy': 'PreemptLowerPriority',
               'description': 'Isolated teaching demo, below normal application priority.'}, name + '.yaml')
    available = available_cpu()
    low = 'low-' + RUN
    high = 'high-' + RUN
    apply(pod(low, f'{available - 500}m', priority='devops-low'), 'priority-low-pod.yaml')
    initial = wait_phase(low, 'Running')
    snapshot('priority-before', NS)
    apply(pod(high, '1000m', priority='devops-high', work=True), 'priority-high-pod.yaml')
    observed = []
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        current = obj('get', 'pods', '-n', NS)
        observed.append({'observed_at': time.time(), 'pods': current})
        events = obj('get', 'events', '-n', NS)
        preempted = [e for e in events['items'] if e.get('involvedObject', {}).get('uid') == initial['metadata']['uid']
                     and e.get('reason') == 'Preempted']
        done = any(p['metadata']['name'] == high and p['status']['phase'] == 'Succeeded' for p in current['items'])
        if preempted and done:
            break
        time.sleep(0.3)
    else:
        save('priority-observations.json', observed)
        snapshot('priority-failed', NS)
        raise AssertionError('Scheduler Preempted event and high-priority completion not both observed')
    save('priority-observations.json', observed)
    save('priority-result.json', {'victim_uid': initial['metadata']['uid'], 'preemption_events': preempted,
                                 'high_completed': True, 'passed': True})
    snapshot('priority-after', NS)
    print('SCHEDULER PREEMPTION PASSED', flush=True)


def quota():
    ns = 'devops-quota'
    namespace(ns)
    resource = {'apiVersion': 'v1', 'kind': 'ResourceQuota', 'metadata': {'name': 'budget', 'namespace': ns},
                'spec': {'hard': {'requests.cpu': '500m', 'requests.memory': '128Mi',
                                  'limits.cpu': '1', 'limits.memory': '256Mi', 'pods': '3'}}}
    apply(resource, 'quota.yaml')
    first = pod('quota-first-' + RUN, '250m', ns=ns, mem='64Mi', limitmem='128Mi')
    second = pod('quota-second-' + RUN, '400m', ns=ns, mem='96Mi', limitmem='192Mi')
    apply(first, 'quota-first.yaml')
    wait_phase(first['metadata']['name'], 'Running', ns)
    save('quota-before.json', obj('get', 'resourcequota', 'budget', '-n', ns))
    p = k('create', '-f', '-', data=json.dumps(second), check=False)
    save('quota-rejection.txt', p.stderr + p.stdout)
    if p.returncode == 0 or 'exceeded quota' not in p.stderr or 'Forbidden' not in p.stderr:
        raise AssertionError('Expected admission rejection not observed')
    if k('get', 'pod', second['metadata']['name'], '-n', ns, check=False).returncode == 0:
        raise AssertionError('Rejected Pod exists')
    k('delete', 'pod', first['metadata']['name'], '-n', ns, '--wait=true')
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = obj('get', 'resourcequota', 'budget', '-n', ns)
        if state.get('status', {}).get('used', {}).get('requests.cpu') == '0':
            break
        time.sleep(1)
    save('quota-released.json', state)
    apply(second, 'quota-second.yaml')
    wait_phase(second['metadata']['name'], 'Running', ns)
    save('quota-after.json', obj('get', 'resourcequota', 'budget', '-n', ns))
    snapshot('quota-after', ns)
    k('delete', 'pod', second['metadata']['name'], '-n', ns, '--wait=true')
    save('quota-result.json', {'admission_rejection': True, 'rejected_pod_absent': True,
                              'same_spec_admitted_after_release': True})
    print('RESOURCE QUOTA PASSED', flush=True)


if __name__ == '__main__':
    cpu_pending()
    preemption()
    quota()
