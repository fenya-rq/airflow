from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from deepdiff import DeepDiff

PYTHON_PATH = '/usr/python/bin/python3'

# TODO: read creds from .env
PG_DEV = 'postgresql://postgres:password@host.docker.internal:5432/postgres'
PG_PROD = 'postgresql://postgres:password@host.docker.internal:5433/postgres'

user_args = {
    'dag_display_name': 'compare_dbses',
    'description': 'test description',
    'catchup': False,
    'tags': ['check_'],
}

@dag(dag_id='1', **user_args)
def validate_schemas():

    @task.external_python(task_id='get_schema', python=PYTHON_PATH)
    def get_schema(conn_str: str) -> list:
        from psycopg import connect

        print('=== Start getting schema... ===')

        with connect(conn_str) as conn:
            rows = conn.execute(
                """
                SELECT 
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    format_type(a.atttypid, a.atttypmod) AS data_type,
                    NOT a.attnotnull AS is_nullable,
                    pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
                    CASE 
                        WHEN pk.contype = 'p' THEN 'PRIMARY KEY'
                        WHEN fk.contype = 'f' THEN 'FOREIGN KEY'
                        WHEN uq.contype = 'u' THEN 'UNIQUE'
                    END AS constraint_type,
                    COALESCE(pk.conname, fk.conname, uq.conname) AS constraint_name,
                    fk_table.relname AS foreign_table_name,
                    fk_attr.attname AS foreign_column_name
                FROM pg_namespace n
                JOIN pg_class c ON n.oid = c.relnamespace
                JOIN pg_attribute a ON c.oid = a.attrelid
                LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                LEFT JOIN pg_constraint pk ON pk.conrelid = c.oid AND a.attnum = ANY(pk.conkey) AND pk.contype = 'p'
                LEFT JOIN pg_constraint fk ON fk.conrelid = c.oid AND a.attnum = ANY(fk.conkey) AND fk.contype = 'f'
                LEFT JOIN pg_constraint uq ON uq.conrelid = c.oid AND a.attnum = ANY(uq.conkey) AND uq.contype = 'u'
                LEFT JOIN pg_class fk_table ON fk.confrelid = fk_table.oid
                LEFT JOIN pg_attribute fk_attr ON fk.confrelid = fk_attr.attrelid 
                    AND fk_attr.attnum = fk.confkey[array_position(fk.conkey, a.attnum)]
                WHERE n.nspname = 'public'
                  AND c.relkind = 'r'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND (c.relname ~ 'plan' OR c.relname = 'auth_user')
                ORDER BY c.relname, a.attnum;
                """
            ).fetchall()

        print('=== Schema got successfully! ===')
        return rows

    @task.external_python(task_id='transform_schema', python=PYTHON_PATH)
    def transform_schema(schema_data: list) -> dict:

        print('=== Start schema transformation... ===')

        schema = {}
        for (schema_name, table, column, dtype, nullable, default,
             ctype, cname, ftable, fcol) in schema_data:

            t = schema.setdefault(table, {"columns": {}, "constraints": []})

            t["columns"][column] = {
                "data_type": dtype,
                "is_nullable": nullable,
                "default": default
            }

            if ctype:
                t["constraints"].append({
                    "column": column,
                    "type": ctype,
                    "name": cname,
                    "foreign_table": ftable,
                    "foreign_column": fcol
                })

        print('=== Transformation ended! ===')
        return schema

    @task.external_python(task_id='compare_schema', python=PYTHON_PATH)
    def diff_schemas(schema_a: dict, schema_b: dict) -> dict:
        return DeepDiff(
            schema_a,
            schema_b,
            ignore_order=True,
            report_repetition=True,
            verbose_level=2
        )

    @task.branch
    def check_diff(data: dict):
        if data:
            print('=== diff detected! ===')
            return "handle_diff"
        else:
            print('=== no diff ===')
            return "no_diff"

    handle_diff = EmptyOperator(task_id="handle_diff")
    no_diff = EmptyOperator(task_id="no_diff")

    dev_schema_data = get_schema(PG_DEV)
    prod_schema_data = get_schema(PG_PROD)

    dev_schema = transform_schema(dev_schema_data)
    prod_schema = transform_schema(prod_schema_data)

    diff = diff_schemas(dev_schema, prod_schema)
    next_step = check_diff(diff)
    next_step >> [handle_diff, no_diff]


validate_schemas()
