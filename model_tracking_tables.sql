create or replace sequence ml_model_runs_sequence start = 1 increment = 1;

create or replace table ml_model_runs(run_id integer,
                                      table_name varchar(256),
                                      algorithm varchar(100),
                                      training_parameters variant,
                                      start_time timestamp, 
                                      end_time timestamp,
                                      model_object variant);
