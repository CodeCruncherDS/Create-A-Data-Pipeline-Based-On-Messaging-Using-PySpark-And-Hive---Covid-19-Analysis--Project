import airflow
from airflow.models.import DAG
from airflow.operators.hive_operator
import HiveOperator
from airflow.utils.dates
import days_ago

dag_hive = DAG(dag_id = "hive_script",
  schedule_interval = '* * * * *',
  start_date = airflow.utils.dates.days_ago(1))

hql_query = """use default;
CREATE EXTERNAL TABLE IF NOT EXISTS airflow_hive(
`Global_new_confirmed` int,
`Global_new_deaths` int,
`Global_new_recovered` int,
`Global_total_confirmed` int,
`Global_total_deaths` int,
`Global_total_recovered` int,
`Country_code` string,
`Country_name` string,
`Country_new_deaths` int,
`Country_new_recovered` int,
`Country_newconfirmed` int,
`Country_slug` string,
`Country_total_confirmed` int,
`Country_total_deaths` int,
`Country_total_recovered` int,
`Extracted_timestamp` timestamp);
insert into airflow_hive from corona;
"""

hive_task = HiveOperator(hql = hql_query,
  task_id = "hive_script_task",
  hive_cli_conn_id = "hive_local",
  dag = dag_hive
)

hive_task

if__name__ == '__main__ ':
  dag_hive.cli()