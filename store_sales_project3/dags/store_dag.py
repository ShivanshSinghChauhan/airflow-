from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator


store_dag = DAG(**{"dag_id": "store_DAG", "start_date": "'2020-6-30'"})


clean_raw_csv = PythonOperator(
    **{"python_callable": "data_cleaner", "task_id": "clean_raw_csv"},
    dag=store_dag
)
insert_into_table = SqliteOperator(
    **{
        "sql": "insert_into_table.sql",
        "sqlite_conn_id": "mysql_conn",
        "task_id": "insert_into_table",
    },
    dag=store_dag
)
check_file_exists = BashOperator(
    **{
        "bash_command": "shasum ~/store_files_airflow/raw_store_transactions_2020-06-30.csv",
        "task_id": "check_file_exists",
    },
    dag=store_dag
)
create_mysql_table = SqliteOperator(
    **{
        "sql": "create_table.sql",
        "sqlite_conn_id": "mysql_conn",
        "task_id": "create_mysql_table",
    },
    dag=store_dag
)
select_from_table = SqliteOperator(
    **{
        "sql": "select_from_table.sql",
        "sqlite_conn_id": "mysql_conn",
        "task_id": "select_from_table",
    },
    dag=store_dag
)
move_file1 = BashOperator(
    **{
        "bash_command": "cat ~/store_files_airflow/location_wise_profit.csv && mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date",
        "task_id": "move_file1",
    },
    dag=store_dag
)
move_file2 = BashOperator(
    **{
        "bash_command": "cat ~/store_files_airflow/store_wise_profit.csv && mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date",
        "task_id": "move_file2",
    },
    dag=store_dag
)
send_email = EmailOperator(
    **{
        "to": "shivanshsinghchauhan@gmail.com",
        "subject": "Daily report generated",
        "html_content": "<h1>Congratulations! Your store reports are ready.</h1>",
        "task_id": "send_email",
    },
    dag=store_dag
)
rename_raw = BashOperator(
    **{
        "bash_command": "mv ~/store_files_airflow/raw_store_transactions_2020-06-30.csv ~/store_files_airflow/raw_store_transactions_2020-06-31.csv",
        "task_id": "rename_raw",
    },
    dag=store_dag
)


check_file_exists >> clean_raw_csv >> create_mysql_table >> insert_into_table >> select_from_table >> move_file1 >> send_email >> rename_raw
select_from_table >> move_file2 >> send_email
