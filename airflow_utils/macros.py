# Common User Defined Macros
# 
# This module contains common user defined macros for use with Apache Airflow.
# This is an example of how to use this module and make these macros available
# in your DAG:
#
# from macros import common_macros
# dag = DAG("glycemic_features",
#          default_args=default_args,
#          schedule_interval="15 18 * * *",
#          user_defined_macros=common_macros,
#          )
#
# t1 = BashOperator(task_id='example,
#        bash_command="echo {{ include_file_as_html }}",
#        dag=dag) 
#
# To add more common user defined macros, add them as functions below.  If you
# want to use a helper function for your macro, prefix the name with _ or __
# and it will not be included in common_macros.

from html import escape

##############################################################################
# Begin common user defined macros

def include_file_as_html(name):
    with open(name) as f:
        return escape(f.read()).replace("\n", "<br>")

# End common user defined macros
##############################################################################
# All macros must be defined above this line
common_macros = {k: v 
                 for k, v in globals().items() 
                    if callable(v) and not k.startswith('_')}
