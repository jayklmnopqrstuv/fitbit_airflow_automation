(select PARSE_DATE("%F", '{{ ds }}') as ds,
* from odp_level2_feature_store_dev.fitbit_features_hrv_1h)
union all
(select PARSE_DATE("%F", '{{ ds }}') as ds,
* from odp_level2_feature_store_dev.fitbit_features_hrv_3h)
union all
(select PARSE_DATE("%F", '{{ ds }}') as ds,
* from odp_level2_feature_store_dev.fitbit_features_hrv_6h)
union all
(select PARSE_DATE("%F", '{{ ds }}') as ds,
* from odp_level2_feature_store_dev.fitbit_features_hrv_12h)
