FROM python:3.11-slim

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /workspace/quality

CMD ["python", "/workspace/quality/run_validation.py"]
