| Start Date | Description |
| ---------- | ----------- |
| 2023-06-09 | Title       |

# Overview
Build a pipeline to extract, load and transform the data. According the data movess for each layer, the quality will increase.
We are going to scrape data from the website [FIPE](https://veiculos.fipe.org.br/).
- **Goal**: Deploy the pipeline into Databricks
- **Non-Goals**: Pay attention if raises exception, because `xpaths` changes a lot.
- **Milestone** Install a Google Chrome browser into the Job Clusters to do the scraper for us.
- **Main Audience** : Other interested engineers and I.
# Requirements ( High Level functional requirement )
1. Run the pipeline once a month.
2. Use [Delta Lake](https://delta.io/) as Storage framework.
- Databricks workspace
- Python scripts will run in Databricks as Workflows to make all the tasks.
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
1. Create pipeline folder and add our scripts python to make the tasks
2. Set dbx deployment file.
3. Deploy it utilizing [dbx](https://dbx.readthedocs.io/en/latest/)
4. Schedule the jobs to run once a month.
# Data Assets
# Drawbacks/Constraints
For being a Scraper we need to Get content, save as Delta, read later again to reuse in other steps.

# Alternatives
# Adoption Strategy
# Open Questions
# References


