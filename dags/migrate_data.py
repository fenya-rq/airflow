import os

from airflow.sdk import dag, task

PROD_CONFIG = {
    'SERVER_USER': os.getenv('PROD_SERVER_USER'),
    'SERVER_HOST': os.getenv('PROD_SERVER_HOST'),
    'PATH_TO_SOURCE': os.getenv('PROD_PATH_TO_SOURCE'),
    'PATH_TO_DESTINATION': os.getenv('PROD_PATH_TO_DESTINATION'),
    'SSH_PORT': os.getenv('PROD_SSH_PORT'),
    'SSH_KEY_PATH': os.getenv('PROD_SSH_KEY_PATH'),
}

user_args = {
    'dag_display_name': 'migrate_data',
    'description': 'migrate data',
    'catchup': False,
    'tags': ['migration_'],
}

@dag(dag_id='1', **user_args)
def migrate_data():

    @task.bash(env=PROD_CONFIG)
    def extract():

        return '''
        set -e
        echo "--- Начинаем копирование дампа ---"

        # 1. Проверка существования и прав ключа
        if [ -f "${SSH_KEY_PATH}" ]; then
            echo "SSH Key найден. Проверка прав (должно быть 600 или 400):"
            ls -l "${SSH_KEY_PATH}"
        else
            echo "ERROR: SSH Key Не найден в директории ${SSH_KEY_PATH}. Проверьте монтирование тома с ключами!"
            exit 1
        fi

        # 2. Проверка сетевого соединения
        echo "Проверка подключения к ${SERVER_HOST}:${SSH_PORT}..."
        if nc -zvw 5 "${SERVER_HOST}" "${SSH_PORT}"; then
            echo "Тест поключения успешен, порт доступен."
        else
            echo "ERROR: Тест поключения провалился. Проверьте настройки Docker, сети или фаейрволла."
            exit 1
        fi

        # 3. Основная команда rsync
        echo "--- Running rsync command ---"

        rsync -avz \
        -e "ssh -i ${SSH_KEY_PATH} -p ${SSH_PORT} -o StrictHostKeyChecking=no" \
        ${SERVER_USER}@${SERVER_HOST}:${PATH_TO_SOURCE} \
        ${PATH_TO_DESTINATION}

        echo "--- Копирование дампа завершено. ---"
        '''

    extract()


migrate_data()
