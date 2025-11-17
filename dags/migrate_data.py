import os

from airflow.models import Variable
from airflow.sdk import dag, task

SERVER_USER = os.getenv('SERVER_USER')
SERVER_HOST = os.getenv('SERVER_HOST')
PATH_TO_SOURCE = os.getenv('PATH_TO_SOURCE')
PATH_TO_DESTINATION = os.getenv('PATH_TO_DESTINATION')
SSH_PORT = os.getenv('SSH_PORT')
SSH_KEY_PATH = os.getenv('SSH_KEY_PATH')

user_args = {
    'dag_display_name': 'migrate_data',
    'description': 'migrate data',
    'catchup': False,
    'tags': ['migration_'],
}

@dag(dag_id='1', **user_args)
def migrate_data():

    @task.bash()
    def extract():

        return f'''
        set -e
        
        rsync -avz \
        -e "ssh -i {SSH_KEY_PATH} -p {SSH_PORT}" \
        {SERVER_USER}@{SERVER_HOST}:{PATH_TO_SOURCE} \
        {PATH_TO_DESTINATION}
        '''

    extract()


migrate_data()
