create index on "2024-02" (dno_alias);
create index on "2024-02" (secondary_substation_id);
create index on "2024-02" (secondary_substation_name);
create index on "2024-02" (data_collection_log_timestamp);
create index on "2024-02" (substation_latitude);
create index on "2024-02" (substation_longitude);
create index on "2024-02" (dno_alias, secondary_substation_id, secondary_substation_name, substation_latitude, substation_longitude, data_collection_log_timestamp);