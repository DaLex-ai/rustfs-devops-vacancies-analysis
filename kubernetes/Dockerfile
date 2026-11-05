FROM python:3.11-slim@sha256:1d6131b5d479888b43200645e03a78443c7157efbdb730e6b48129740727c312
WORKDIR /app
COPY kubernetes/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY check_storage.py mapreduce_devops.py ./
COPY results/mapreduce/skills_2.csv reference/skills_2.csv
COPY kubernetes/worker.py kubernetes/worker.py
RUN mkdir -p results/mapreduce evidence && chown -R 10001:10001 /app
USER 10001:10001
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "kubernetes/worker.py"]
