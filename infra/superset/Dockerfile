FROM apache/superset:4.1.1

USER root

RUN pip install --no-cache-dir trino sqlalchemy-trino

COPY superset_config.py /app/pythonpath/superset_config.py
COPY init-superset.sh /app/init-superset.sh
RUN sed -i 's/\r$//' /app/init-superset.sh && chmod +x /app/init-superset.sh

USER superset

CMD ["bash", "/app/init-superset.sh"]
