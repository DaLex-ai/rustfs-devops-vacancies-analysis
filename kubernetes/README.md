# Kubernetes: RustFS и параллельный MapReduce

## Архитектура и файлы

- `kind.yaml`: отдельный кластер `devops-k8s`, kind 0.33.0 / Kubernetes 1.35.8.
- `Dockerfile`, `Dockerfile.dockerignore`, `requirements.txt`: Python 3.11, базовый образ по digest,
  точные версии зависимостей; в образ попадают только адаптер, исходный mapper, S3-клиент и эталон пар.
- `worker.py`: режимы `single`, `split`, `map`, `reduce`. Импортирует **неизменённую**
  `mapreduce_devops.map_chunk`. Она вычисляет локальные счётчики размеров 1/2/3;
  адаптер агрегирует и сохраняет глобальный счётчик пар.
- `run.py`: развёртывание, создание Secret без записи ключей на диск, миграция,
  запуск Job, сохранение evidence, проверка перезапуска RustFS.
- `scheduling.py`: изолированные эксперименты с CPU, preemption и квотами.
- `audit.py`: повторная независимая проверка сохранённых статусов и трёх CSV-эталонов.
- `bootstrap.ps1`: развёртывание и эксперименты с нового Kubernetes-окружения.
- `manifests/`: использованные спецификации Kubernetes. Файлы `.yaml` записаны в JSON-синтаксисе,
  который является допустимым YAML; `kubectl apply -f` принимает их непосредственно.
- `report/`: PDF, данные отчёта и скрипт его генерации.

Пространства имён: `devops-compute`, `devops-scheduling`, `devops-quota`.
RustFS использует ClusterIP Service `rustfs:9000`, StatefulSet и четыре PVC по 3 GiB.
Запись локальных журналов разрешена через `emptyDir`; данные находятся на PVC.
Ключи передаются через Secret `rustfs-auth`; kubeconfig хранится вне репозитория.

CSV разбивается по **логическим записям** через `csv.DictReader`, round-robin на 3 части.
Сохраняются все поля всех записей. Проверяются количество и SHA-256 канонического
представления записей каждого фрагмента. Mapper читает только свой объект из S3.
Reducer читает ровно один результат каждого индекса, суммирует частоты и сравнивает
полный словарь пар с эталоном. При равной частоте топ-10 упорядочивается по названиям пары.

## Предварительные условия

Windows 11, Docker Desktop с Linux Engine, Python 3.11, доступ в интернет для образов
и инструментов. Для стенда нужно около 5 GiB свободной памяти и запас свободного диска
не менее 15 GiB. Перед запуском Docker Desktop должен быть запущен.

Нужен исходный датасет ДЗ №3 в доступном S3: `hh-lake/raw/vacancies.csv`.
Этот файл не включён в Git и не создаётся из эталонных результатов. В локальной среде
используется существующий `rustfs-server`. На другой машине надо сначала восстановить
хранилище предыдущей работы либо указать доступный S3 через `RUSTFS_ENDPOINT`,
`RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`. Все три значения задаются в локальном окружении;
реальные ключи не записываются в README или манифесты. Для Docker-baseline внешний S3
по умолчанию доступен как `http://host.docker.internal:9000`.

## Полный воспроизводимый запуск

Из корня репозитория в PowerShell:

```powershell
.\kubernetes\bootstrap.ps1
```

Скрипт скачивает kind и kubectl с официальных сайтов и проверяет SHA-256, создаёт
отдельный kubeconfig, собирает образ и выполняет этапы последовательно. Выключенные
посторонние контейнеры не запускает; удаляет только собственные экспериментальные Pod.
Docker Desktop сам может запустить контейнеры с существующей restart policy.
Скрипт не выполняет git add/commit/push и не удаляет кластеры, PVC, volumes или датасеты.

Каждый новый прогон требует нового `LAB_RUN`; bootstrap генерирует его автоматически.
Повторный запуск Job с тем же именем запрещён, чтобы старый успех нельзя было принять
за новый результат. Исходный файл мигрируется в отдельный кластер; старое S3 сохраняется.

### Поэтапный запуск в уже подготовленном окружении

```powershell
$env:LAB_RUN = Get-Date -Format 'yyyyMMdd-HHmmss'
$env:PYTHONIOENCODING = 'utf-8'
.\.venv\Scripts\python.exe kubernetes/run.py setup
.\.venv\Scripts\python.exe kubernetes/run.py baseline
.\.venv\Scripts\python.exe kubernetes/run.py migrate
.\.venv\Scripts\python.exe kubernetes/run.py compute
.\.venv\Scripts\python.exe kubernetes/run.py persistence
.\.venv\Scripts\python.exe kubernetes/scheduling.py
.\.venv\Scripts\python.exe kubernetes/audit.py
```

Для повторной проверки **сохранённого** прогона, без повторного расчёта:

```powershell
$env:LAB_RUN = '20261030-01'
.\.venv\Scripts\python.exe kubernetes/audit.py
$env:KUBECONFIG = 'C:\rustfs-lab\k8s-tools\devops-kubeconfig'
& 'C:\rustfs-lab\k8s-tools\kubectl.exe' get nodes
& 'C:\rustfs-lab\k8s-tools\kubectl.exe' get jobs,pods,pvc -n devops-compute
& 'C:\rustfs-lab\k8s-tools\kubectl.exe' get resourcequota -n devops-quota
```

## Память и интерпретация результатов

Метрика контейнеров - cgroup v2 `memory.peak`, прочитанная после вычисления и записи
результата перед завершением. Это пик до точки чтения, а не периодическая выборка RSS.
Для убитого процесса итоговый счётчик недоступен; лимит нельзя выдавать за измеренный пик.
`restartPolicy: Never`, `backoffLimit: 0` сохраняют первоначальный исход эксперимента.
Предел подбирается как `ceil(peak_MiB * 1.5 / 16) * 16`.

Windows Peak Working Set из ДЗ №3 (87,97 MiB) и cgroup memory.peak измеряют разные вещи:
cgroup включает учтённую память группы, в том числе файловый кэш и память ядра; Working Set
описывает резидентные страницы процесса. Кроме того, ДЗ №3 удерживало глобальные счётчики
одиночных навыков, пар и троек, а текущий адаптер удерживает глобально только пары.
Поэтому разницу нельзя объявлять выигрышем Kubernetes в памяти.

Разбиение пишет временные CSV на диск; его пик (~928 MiB) включает файловый кэш,
поэтому Job разбиения имеет отдельный лимит 1 GiB и не входит в подбор памяти mapper.
Суммарная стоимость split/map/reduce и запуска Pod не равна чистому времени mapper.
Один замер не доказывает общего ускорения конвейера.
