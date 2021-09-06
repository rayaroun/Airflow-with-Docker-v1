try:

	from datetime import timedelta
	from airflow import DAG
	from airflow.operators.python_operator import PythonOperator
	from datetime import datetime
	import pandas as pd
	# from datetime import datetime
	print("All dag modules are okay")

except Exception as e:
	print("Error {}".format(e))


def first_function_execute(*args, **kwargs):

	variable = kwargs.get("name", "Didn't get the key")

	print("Hello {}".format(variable))
	return "Hello + " + variable


with DAG(

	dag_id = "first_dag",

	schedule_interval = "@daily",

	default_args = {

		"owner" : "airflow",
		"retries" : 1,
		"retry_delay" : timedelta(minutes=5),
		"start_date" : datetime(2021,1,1)
	},
	#telling it to skip all the previous runs
	catchup = False) as f:

	first_function_execute = PythonOperator(

		task_id = "first_function_execute",
		python_callable = first_function_execute,
		op_kwargs = {"name" : "Aroun Dalawat"}
		
		)





