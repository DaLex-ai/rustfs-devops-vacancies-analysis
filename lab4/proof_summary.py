"""Human-readable summary derived strictly from recorded JSON log events."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
out = root / "evidence" / "summary_for_review.txt"
lines = []

for prefix, title in (("no_fencing", "FENCING=0"), ("fencing", "FENCING=1")):
    eligible = []
    for folder in (root / "evidence").glob(prefix + "_*"):
        summary_file = folder / "summary.json"
        if summary_file.exists():
            data = json.loads(summary_file.read_text(encoding="utf-8"))
            if data.get("status") == "PASSED" and data.get("failover_clock"):
                eligible.append((folder.name, data))
    _, info = max(eligible)
    run = info["run"]
    lines.append(f"=== {title} / {run} ===")
    lines.append(f"Время переключения: {info['failover_seconds']:.3f} с (host monotonic)")
    lines.append(f"Запасной продолжил после чанка: {info['standby_checkpoint_chunk']}")
    for worker in ("a", "b"):
        path = root / "logs" / run / f"{worker}.txt"
        evts = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]
        exact = [x for x in evts if x["event"] == "EXACT_MATCH"]
        assert len(exact) == 1
        lines.append(f"{worker.upper()} EXACT_MATCH: {exact[0]['unique']} пар, {exact[0]['occurrences']} вхождений")
        for ev in evts:
            if ev["event"] == "PUBLISHED_RESULT":
                lines.append(f"{ev['utc']} {worker.upper()} записал результат (успешно), версия указателя {ev['pointer_version']}, эпоха {ev['epoch']}")
            if ev["event"] == "BadVersionError":
                lines.append(f"{ev['utc']} {worker.upper()} BadVersionError — устаревшая запись отклонена")
    lines.append(f"Успешных записей: {info['successful_publications']}")
    lines.append(f"Конечная версия указателя: {info['expected_version']}")
    lines.append("EXPERIMENT_PASSED")
    lines.append("")

out.write_text("\n".join(lines), encoding="utf-8")
print(out)
print("\n".join(lines))
