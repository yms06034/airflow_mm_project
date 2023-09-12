# Login_session initialization task every midnight

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta
import pendulum


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner' : 'KSJ_mm',
    'start_date': datetime(2023, 8, 25, 0, 0, 0, tzinfo=kst),
    'retries' : 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='macro_loginSession_change',
    default_args=default_args,
    schedule_interval=timedelta(hours=4),
    tags=['macro'],
    catchup=False
) as dag:

    mysql_conn_id = 'mmkdb_mysql_conn'

    sql_query = """
    SELECT id
    FROM n_c_home_userinfo
    WHERE login_session = 1;
    """
    
    run_sql_query = MySqlOperator (
        task_id='run_sql_query',
        mysql_conn_id=mysql_conn_id,
        sql=sql_query,
        do_xcom_push=True,
        doc_md = 'Macro Login Session SELECT',
    )

    
    def update_using_ids(**kwargs):
        ti = kwargs['ti']
        select_id = ti.xcom_pull(task_ids='run_sql_query')
        
        ids = [r_id[0] for r_id in select_id]
        
        if ids:
            update_query = f"""
            UPDATE n_c_home_userinfo
            SET login_session = 0
            WHERE id IN ({','.join(['%s'] * len(ids))});
            """
            mysql_hook = MySqlHook(mysql_conn_id = mysql_conn_id)
            mysql_hook.run(update_query, parameters=ids)
            
    update_using_ids_t = PythonOperator(
        task_id='update_using_ids_t',
        python_callable=update_using_ids,
        provide_context=True,
        doc_md = 'Macro Login Session UPDATE',
    )
    
    
    
    run_sql_query >> update_using_ids_t
