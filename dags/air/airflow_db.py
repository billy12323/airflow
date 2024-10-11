
# 使用airflow的pg数据库连接
from airflow.models.connection import Connection
from airflow.secrets.local_filesystem import LocalFilesystemBackend
# conn info need store in secretbackend i.e LocalFilesystemBackend where config file path in airflow.cfg
c = Connection(conn_id="awesome_conn",description="Example Connection",conn_type="postgres")
print(c.test_connection())
hook = c.get_hook()
output = hook.run(
            sql="SELECT 1",
            autocommit=True,
            parameters=None,
            handler=None,
        )
print(output)


# 使用airflow的dagbag触发执行dag
from airflow.models.dagbag import DagBag

dagbag = DagBag(read_dags_from_db=True)
dag_id = 'example_bash_operator'
dag = dagbag.get_dag(dag_id)
print(dag)
# airflow/api_connexion/endpoints/dag_run_endpoint.py
# dag_run = dag.create_dagrun(
#                 run_type=DagRunType.MANUAL,
#                 run_id=run_id,
#                 execution_date=logical_date,
#                 data_interval=data_interval,
#                 state=DagRunState.QUEUED,
#                 conf=post_body.get("conf"),
#                 external_trigger=True,
#                 dag_hash=get_airflow_app().dag_bag.dags_hash.get(dag_id),
#                 session=session,
#                 triggered_by=DagRunTriggeredByType.REST_API,
#             )


# airflow/utils/airflow_flask_app.py
# airflow/www/extensions/init_dagbag.py
from typing import Any, cast
class Base:
    def __init__(self):
        print('base init')

class AirflowApp(Base):
    """Airflow Flask Application."""

    dag_bag: DagBag
    api_auth: list[Any]

bb = Base()
bb.dag_bag = DagBag(read_dags_from_db=True)
aa = cast(AirflowApp, bb)
print(aa)