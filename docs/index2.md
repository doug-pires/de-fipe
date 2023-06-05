# Use Case
## System
Build an ELT code to extract data from FIPE. We are going to build the Scraper
## Actors
Databricks workspace will host our pipeline
Python scripts will run in Databricks as Workflows to make all the tasks.
## Scenario
1. Set the local enviroment
 - Poetry to manage our metadata project
 - Config Github
2. Install libraries we are going to use.
 - pyspark
 - delta-spark
 - pytest
 - dbx
 - among others
3. Create Configuration file for holding Dataframe names, schema and base paths.
4. Create Python Scripts
 - Install the webbrowser on the cluster
 - Code extract scripts
 - Code load scripts
  - Load as Delta Lake
 - Code transformation scripts
  - Create Data Dictionary for the tables
 - dev scripts for local development
5. Unit testing for the scripts
 - Use `pytest.mark` to mark our files
 - Set `conftest.py` with `SparkSession`
6. Create pipeline folder and add our scripts python to make the tasks
7. Set dbx deployment file.
8. Deploy it
9. Schedule the jobs to run once a month.



## Use Case

