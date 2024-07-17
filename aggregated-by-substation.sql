copy (
    select
        concat_ws('-', dno_alias, secondary_substation_id, secondary_substation_name) as substation,
        round(sum(total_consumption_active_import) / sum(aggregated_device_count_active))  as consumption,
        data_collection_log_timestamp as "timestamp",
        round(cast(substation_latitude as decimal), 5) as latitude,
        round(cast(substation_longitude as decimal), 5) as longitude
    from
        "2024-02"
    where
        data_collection_log_timestamp between '2024-02-12 00:30:00' and '2024-02-13 00:00:00'
    group by
        dno_alias,
        secondary_substation_id,
        secondary_substation_name,
        data_collection_log_timestamp,
        substation_latitude,
        substation_longitude
    order by
        data_collection_log_timestamp,
        dno_alias,
        secondary_substation_id,
        secondary_substation_name
) to '/Users/steve/Code/lv-feeder-smart-meter-data/data/output/2024-02-12-aggregated-by-substation.csv' delimiter ',' csv header;