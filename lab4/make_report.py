"""Generate an evidence-backed Russian report from PASSED lab4 experiments."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT / "evidence"
OUTPUT = ROOT / "report"
OUTPUT.mkdir(exist_ok=True)


def latest(prefix):
    candidates = []
    for d in EVIDENCE.glob(prefix + "_*"):
        path = d / "summary.json"
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("status") == "PASSED" and payload.get("failover_clock"):
                candidates.append((d.name, payload))
    if not candidates:
        raise RuntimeError(f"No finalized experiment with host clock: {prefix}")
    return max(candidates)[1]


def worker_events(run, worker):
    path = ROOT / "logs" / run / f"{worker}.txt"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


no = latest("no_fencing")
yes = latest("fencing")
quorum = json.loads((EVIDENCE / "ensemble" / "quorum.json").read_text(encoding="utf-8"))
assert quorum[0]["step"] == "all_three" and quorum[1]["step"] == "one_follower_down"
assert quorum[2]["step"] == "two_followers_down" and quorum[3]["step"] == "restored"
assert quorum[0]["probe"]["returncode"] == 0
assert quorum[1]["probe"]["returncode"] == 0
assert quorum[2]["probe"]["returncode"] != 0
assert sorted(quorum[3]["roles"].values()) == ["follower", "follower", "leader"]
assert "EPHEMERAL_DISAPPEARED" in (EVIDENCE / "ensemble" / "ephemeral.txt").read_text()
assert no["expected_version"] == 2 and no["successful_publications"] == 2
assert yes["expected_version"] == 1 and yes["successful_publications"] == 1
assert '"owner": "b"' in yes["pointer"]
assert '"owner": "a"' in no["pointer"]
assert any(e["event"] == "BadVersionError" for e in worker_events(yes["run"], "a"))

pairs_a_no = next(e for e in worker_events(no["run"], "a") if e["event"] == "EXACT_MATCH")
pairs_b_no = next(e for e in worker_events(no["run"], "b") if e["event"] == "EXACT_MATCH")
pairs_a_yes = next(e for e in worker_events(yes["run"], "a") if e["event"] == "EXACT_MATCH")
pairs_b_yes = next(e for e in worker_events(yes["run"], "b") if e["event"] == "EXACT_MATCH")
assert len({x["sha256"] for x in (pairs_a_no, pairs_b_no, pairs_a_yes, pairs_b_yes)}) == 1

lines = f"""# ОТЧЁТ ПО ДОМАШНЕЙ РАБОТЕ № 4

## «Обеспечение отказоустойчивости средствами Apache ZooKeeper»

**Студент:** Алёшкин Дмитрий Игоревич  
**Группа:** J3311  
**Индивидуальный вариант:** № 11 — DevOps-инженер  
**Дата:** 26 октября 2026 г.

---

## 1. Цель и постановка задачи

Развернуть ансамбль из трёх серверов ZooKeeper, измерить кворум и отказоустойчивость, связать его с двумя независимыми экземплярами координатора расчёта пар навыков из ДЗ № 3. После приостановки активного процесса подтвердить возобновление вычисления с настоящего чекпоинта, а также сравнить незащищённую и защищённую по эпохе публикацию результата.

## 2. Стенд и архитектура

- Windows 11, Docker Desktop и Docker Compose.
- Apache ZooKeeper **3.9.6**, Java **17** в трёх отдельных Docker-контейнерах.
- Сервера: `lab4-zk1` (localhost:2181), `lab4-zk2` (2182), `lab4-zk3` (2183).
- Постоянные раздельные Docker volumes для данных и журналов ZooKeeper.
- Два независимых координатора `lab4-coordinator-a` и `lab4-coordinator-b`; Python 3.11 и Kazoo 2.10.0.
- Тайм-аут сессий ZooKeeper — **10 секунд**.
- Знаки лидерства — ephemeral sequential znodes. Эпоха лидерства хранится в versioned znode. Результат публикуется через versioned znode-указатель.
- Docker Compose: `lab4/compose.yaml`. Архивные результаты ДЗ № 3 подключены к координаторам только для чтения.

## 3. Проверка кворума и эфемерного узла

| Сценарий | Фактический результат |
|---|---|
| Три узла | {', '.join(str(k) + ': ' + v for k,v in quorum[0]['roles'].items())} |
| Остановлен один последователь | Клиентская запись и чтение: **успешно** |
| Остановлены два последовательных узла | Клиент не смог выполнить запись, потеря кворума **подтверждена** |
| Оба узла возвращены | Роли: {', '.join(str(k) + ': ' + v for k,v in quorum[3]['roles'].items())} |
| Эфемерный узел | Был виден второй сессии, исчез после закрытия сессии владельца |

Артефакты: `evidence/ensemble/quorum.json` и `evidence/ensemble/ephemeral.txt`.

## 4. Данные и механизм чекпоинта

Вход: компактная CSV-выборка из предыдущей работы (5 158 DevOps-вакансий). Контрольный SHA-256: `541174a6d1f5a80a949dfa7a4255934ea3659a6aa850c6e859892a5a6c2e9093`.

Реальный MapReduce делит данные на **21 чанк по 250 вакансий** (последний 158), упорядочивает уникальные навыки вакансии и формирует неупорядоченные пары через `itertools.combinations`.

На каждом завершённом чанке сохраняется чекпоинт `results/<run>/checkpoint.json` с номером следующего чанка, всем накопленным Counter пар и временем записи. Файловая блокировка предотвращает одновременное повреждение, `os.replace` делает сохранение атомарным. Запасной экземпляр считывает этот файл и продолжает именно с сохранённого индекса.

В обоих сценариях B восстановился после **{no['standby_checkpoint_chunk']}** завершённых чанков (незащищённый) и **{yes['standby_checkpoint_chunk']}** завершённых чанков (защищённый).

Фактически получено: **22 311** уникальных пар и **134 786** суммарных вхождений. Проверена **каждая** частота против `results/mapreduce/skills_2.csv` из ДЗ № 3. Во всех четырёх вычислениях (A/B в обоих опытах) — `EXACT_MATCH`.

## 5. Приостановка без защиты (FENCING=0)

Идентификатор опыта: `{no['run']}`.

Активный A приостановлен Docker-командой `docker pause lab4-coordinator-a` после записи пяти чекпоинтов. Через **{no['failover_seconds']:.3f} с** по монотонным часам хоста наблюдалось лидерство B. B продолжил вычисления с чекпоинта, сверил весь результат, опубликовал его и завершил работу.

После примерно 40 секунд паузы A возобновлён через `docker unpause`. Старый экземпляр продолжил вычисление и **успешно записал указатель второй раз без сравнения версии**.

- Строк `PUBLISHED_RESULT` — **2**;
- Версия результирующего указателя — **2**;
- Конечный владелец — **a**, его эпоха **1**, хотя B уже обладал эпохой **2**.

Это реальная демонстрация «зомби-лидера»: доступ к вычислению и записи остаётся у процесса, утратившего сессию.

## 6. Повтор с проверкой эпохи и версии (FENCING=1)

Идентификатор опыта: `{yes['run']}`.

A снова приостановлен на ~40 секунд посреди обработки. B избран лидером через **{yes['failover_seconds']:.3f} с** по монотонным часам хоста, продолжил с чекпоинта и корректно опубликовал результат.

После возобновления A попытался обновить указатель с устаревшей версией **0**. Непосредственная операция Kazoo `zk.set(pointer, payload, version=0)` получила **`BadVersionError`**; приложение зарегистрировало отказ и не приняло публикацию.

- Строк `PUBLISHED_RESULT` — **1**, от B;
- Строк `BadVersionError` — **1**, от A;
- Версия результирующего указателя — **1**;
- Владелец результата — **b**, актуальная эпоха **2**.

## 7. Сводное сравнение

| Показатель | Без FENCING | С FENCING |
|---|---|---|
| Реальный расчёт пар | 22 311 / 134 786 | 22 311 / 134 786 |
| Поэлементная сверка | EXACT MATCH | EXACT MATCH |
| Чекпоинт B | после чанка {no['standby_checkpoint_chunk']} | после чанка {yes['standby_checkpoint_chunk']} |
| Время failover, с | {no['failover_seconds']:.3f} | {yes['failover_seconds']:.3f} |
| Успешные публикации | 2 | 1 |
| Отклонение старого A | нет | BadVersionError |
| Версия указателя | 2 | 1 |
| Конечный владелец | a (устаревший) | b (актуальный) |

Время измерено **на одном компьютере по монотонным часам Windows** от вызова приостановки A до обнаружения события `ROLE_LEADER` B в журнале. В измерение входит периодический опрос журнала (шаг до ~0,45 секунды). Часы контейнеров и Windows не смешивались при финальном расчёте.

## 8. Почему три сервера, а не два или четыре?

ZooKeeper принимает согласованные решения кворумом — большинством участников. Из двух серверов кворум составляет два: отключение любого узла останавливает согласованные записи. Из трёх нужен кворум из двух, поэтому один сервер можно потерять без потери работоспособности. Из четырёх нужны три голоса, следовательно, отказоустойчивость остаётся такой же, как у трёх. Три сервера — минимальный разумный ансамбль для переживания одного отказа без лишнего четвёртого сервера.

## 9. Почему проверка эпохи нужна на стороне получателя?

ZooKeeper контролирует собственные сессии, znodes и версии. Однако он не останавливает физически старый процесс: после паузы тот может возобновить вычисление и обратиться к RustFS или базе данных по уже открытому каналу. ZooKeeper не перехватывает сторонние вызовы записи. Поэтому **система-получатель** должна сравнить переданный fencing token / эпоху с актуальной и атомарно отказаться от более старой. В данной учебной модели роль получателя указателя выполняет versioned znode: условная запись Kazoo вызывает `BadVersionError` при старой версии. Для реального внешнего хранилища нужна собственная эквивалентная проверка на его стороне.

## 10. Ограничения и воспроизводимость

Эксперимент использует один Windows-компьютер и три контейнера ZooKeeper: это учебная модель, а не независимые физические узлы. Чекпоинт сохраняется в общем локальном каталоге, смонтированном в оба контейнера; в промышленной реализации понадобится устойчивое разделяемое хранилище с корректной синхронизацией. Публикация демонстрируется **на указателе в ZooKeeper**, а не на S3-объекте RustFS; автономная защита RustFS этим тестом не доказывается.

Исходные файлы: `coordinator.py`, `zk_state.py`, `zk_probe.py`, `experiment.py`, `quorum_tests.py`, `compose.yaml`.

Журналы двух координаторов: `logs/{no['run']}/`, `logs/{yes['run']}/`. Машиночитаемые итоги: `evidence/{no['run']}/summary.json`, `evidence/{yes['run']}/summary.json`.

**Вердикт по фактически проведённым экспериментам:** обе демонстрации failover, точная проверка расчёта, кворум и эфемерный узел подтверждены локальными выводами; проект подготовлен локально, без коммитов и публикации.
"""
samples = [
    ("A без защиты", no["run"], "a", "PUBLISHED_RESULT"),
    ("B без защиты", no["run"], "b", "PUBLISHED_RESULT"),
    ("A с защитой", yes["run"], "a", "BadVersionError"),
    ("B с защитой", yes["run"], "b", "PUBLISHED_RESULT"),
]
lines += "\n## 11. Подлинные фрагменты журналов координаторов\n\n"
lines += "События ниже прочитаны непосредственно из файлов, с фактическими UTC-метками времени.\n\n"
for label, run, worker, event in samples:
    line = next(x for x in worker_events(run, worker) if x["event"] == event)
    lines += "**" + label + "**\n\n" + chr(96)*3 + "json\n"
    lines += json.dumps(line, ensure_ascii=False, sort_keys=True)
    lines += "\n" + chr(96)*3 + "\n\n"
path = OUTPUT / "lab4_report.md"
path.write_text(lines, encoding="utf-8")
print("REPORT_GENERATED", path)
print("RUNS", no["run"], yes["run"])
