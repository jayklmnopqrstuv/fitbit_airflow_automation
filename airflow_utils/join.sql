SELECT
    gf.pim_id, gf.ds, gf.period_14d, im.insulin, im.biguanides, im.sulfonylureas
FROM 
    odp_level2_feature_store.glycemic_features as gf
LEFT JOIN 
    odp_level2_feature_store.Individual_Medications as im
ON
    gf.pim_id = cast(im.pim_id as int64)
WHERE 
    gf.ds = '{{ ds }}' and im.ds = '{{ ds }}'
