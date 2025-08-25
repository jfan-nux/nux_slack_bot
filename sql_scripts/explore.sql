-- drop table if exists proddb.fionafan.experiment_metrics_results;
select * from proddb.fionafan.experiment_metrics_results 
where insert_timestamp = (select max(insert_timestamp) from proddb.fionafan.experiment_metrics_results) 
and statsig_string = 'INSUFFICIENT_DATA';