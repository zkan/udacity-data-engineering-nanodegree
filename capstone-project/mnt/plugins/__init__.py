from airflow.plugins_manager import AirflowPlugin

import operators


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.StageToRedshiftOperator,
    ]
