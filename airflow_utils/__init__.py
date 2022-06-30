import os

# environment of this airflow deployment
env = os.environ.get("IP__ENV", "dev")

# name of the scratch space storage bucket for this environment
ip_bucket = os.environ.get("IP__BKT__IP", "bkt_level2_ip_dev")

# name of the GBQ feature store for this environment
ip_feature_store = os.environ.get("IP__DS__FEATURE_STORE", "odp_level2_feature_store_dev")

# name of the GBQ ip workspace dataset for this environment
ip_workspace = os.environ.get("IP__DS__IP", "odp_level2_ip_dev")
