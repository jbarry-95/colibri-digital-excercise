Solution consists of a notebook to run on a job cluster then a DLT pipeline.

So your databricks workflow would look like 

--------------           --------------
|            |           |            |
|            |           |            |
|  Job Task  | ------>   |  DLT Task  |
|            |           |            |
|            |           |            |
--------------           --------------

I went with this approach due to DLT not being able to natively or simply handle files that get updated.

We start with a job task for the initial load which reads the csv files and writes them to separate tables per file. We perform a merge on the 3 separate delta tables per file on each run after the first intial load which writes directly to table.
The merge operation simply checks turbine ID and timestamp as the ID and if doesn't exist in the delta table inserts.

The DLT task then has 3 materialized views defined.

The first view is for cleaning and applying expectations on the raw tables and then unioning all 3 tables into one. The expectation rules defined check for any nulls in any column and if there is a null then that row is dropped.
The second view calculates the min, max, std_dev and avg power output over a 24-hour window.
The third view joins the second summary_data view with the first clean_data view and then creates a new column to calculate if the power_output field is outside 2 standard deviations from the mean.
