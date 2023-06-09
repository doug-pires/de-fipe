| Start Date      | Description |
| ----------- | ----------- |
| 2023-06-09  | Title       |

# Summary
Build an ELT code to extract data from website FIPE. We are going to build the Scraper
# Basic Example
- Databricks workspace
- DBFS
- Python scripts will run in Databricks as Workflows to make all the tasks.
# Motivation
# Detailed Design
1. Set the local environment
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
 - Install the browser on the cluster
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
8. Deploy it utilizing [dbx](https://dbx.readthedocs.io/en/latest/)
9. Schedule the jobs to run once a month.
# Data Assets
# Drawbacks/Constraints
For being a Scraper we need to Get content, save as Delta, read later again to reuse in other steps.
## Goals
- Our project will run without errors due to `xpaths` strings.
## Non-Goals
- Pay attention if raises exception, because `xpaths` changes a lot.
# Alternatives
# Adoption Strategy
# Open Questions
# References


