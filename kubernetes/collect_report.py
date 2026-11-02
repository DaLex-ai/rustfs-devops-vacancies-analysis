"""Collect verified evidence without credentials for report generation."""
import json
from pathlib import Path
from run import OUT, BASE, RUN, command

names = ['migration','persistence','memory-comparison','comparison','partition-manifest','audit','scheduler-capacity','cpu-result','priority-result','quota-result','quota-before','quota-released','quota-after','docker-baseline']
data = {name: json.loads((OUT / (name + '.json')).read_text(encoding='utf-8')) for name in names}
for mode in ('measure','safe','split','map','reduce','verify'):
    path = OUT / f'{mode}-{RUN}-summary.json'
    if path.exists():
        data[mode] = json.loads(path.read_text(encoding='utf-8'))
data['oom'] = json.loads((OUT / f'oom-{RUN}-pods.json').read_text(encoding='utf-8'))['items'][0]['status']
node = json.loads((OUT / 'nodes.json').read_text(encoding='utf-8'))['items'][0]
data['node'] = {key:node['status'][key] for key in ('capacity','allocatable','nodeInfo')}
data['quota-error'] = (OUT / 'quota-rejection.txt').read_text(encoding='utf-8')
data['versions'] = json.loads((OUT / 'versions.json').read_text(encoding='utf-8'))
data['build-image'] = [{key:x.get(key) for key in ('Id','Size','Created','RepoDigests')} for x in json.loads((OUT / 'build-image.json').read_text(encoding='utf-8'))]
data['source_commit'] = command(['git','-C',str(BASE),'rev-parse','HEAD']).stdout.strip()
data['branch'] = command(['git','-C',str(BASE),'branch','--show-current']).stdout.strip()
data['run'] = RUN
folder = BASE / 'kubernetes/report'
folder.mkdir(exist_ok=True)
(folder / 'data.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
print('Report data saved')
