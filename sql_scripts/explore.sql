-- drop table if exsits proddb.fionafan.experiment_metrics_results;
select * from proddb.fionafan.experiment_metrics_results
where insert_date = (select max(insert_date) from proddb.fionafan.experiment_metrics_results);