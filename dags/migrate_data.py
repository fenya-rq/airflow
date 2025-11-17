import os

from airflow.sdk import dag, task

SERVER_USER = os.getenv('SERVER_USER')
SERVER_HOST = os.getenv('SERVER_HOST')
PATH_TO_SOURCE = os.getenv('PATH_TO_SOURCE')
PATH_TO_DESTINATION = os.getenv('PATH_TO_DESTINATION')
SSH_PORT = os.getenv('SSH_PORT')

user_args = {
    'dag_display_name': 'migrate_data',
    'description': 'migrate data',
    'catchup': False,
    'tags': ['migration_'],
}

@dag(dag_id='1', **user_args)
def migrate_data():

    @task.bash(default_args=user_args)
    def extract():
        # local testing
        return f'''
        rsync -avz -e "ssh -p {SSH_PORT}" {SERVER_USER}@{SERVER_HOST}:{PATH_TO_SOURCE} {PATH_TO_DESTINATION}
        '''
        # for prod
        return f'''
        rsync -avz -e "ssh -i /opt/keys/pg_server_key -p {SSH_PORT}" {SERVER_USER}@{SERVER_HOST}:{PATH_TO_SOURCE} {PATH_TO_DESTINATION}
        '''


migrate_data()
