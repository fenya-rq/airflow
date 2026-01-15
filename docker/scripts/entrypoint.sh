#!/usr/bin/env bash
set -e

echo " === Starting custom entrypoint === "

# Функция для ожидания БД (чтобы init не упал, пока Postgres стартует)
wait_for_db() {
  echo "Waiting for Postgres..."
  python << EOF
import sys
import psycopg2
import time
try:
    while True:
        try:
            psycopg2.connect("$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
            break
        except Exception:
            time.sleep(1)
except Exception as e:
    print(e)
    sys.exit(1)
EOF
  echo "Postgres is up!"
}

# 1. Создание необходимых директорий
mkdir -p ${AIRFLOW_HOME}/{dags,logs,plugins,keys,venvs}


# 2. Логика инициализации
if [ "$1" = "init" ]; then
    echo "===== Initializing Airflow Database ====="
    airflow db migrate
    echo "===== Init Finished ====="
    exit 0
fi

# 3. Во всех остальных случаях просто пробрасываем команду
# Это позволит запускать "api-server", "scheduler", "worker", "triggerer"
exec airflow "$@"