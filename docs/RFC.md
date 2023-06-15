| Start Date |
| ---------- |
| 2023-06-09 |

# Overview
Build a pipeline to extract, load and transform the data. According the data moves for each layer, the quality will increase.
We are going to scrape data from the website [FIPE](https://veiculos.fipe.org.br/) which hold Average Vehicle Price.
One instance : I've bought a car. The brand is **DPIRRE** , model **Azx - 92** , Manufacturing Year is **2019** and the kind of fuel is **Gasoline**

| Brand   | Model    | Manufacturing Year |  Fuel    | Price ( euros )  |
|---------| ---------| ------------------ |----------|------------------|
| DPIREE| | Azx - 92 |       2019         | Gasoline |       27.000     |

Over time my vehicle suffers depreciation or appreciation ( hard to happen ).

- **Goals**: 
  - Deploy the pipeline into Databricks
  - Schedule it
  - Build a report on top of Delta tables
- **Non-Goals**: Pay attention if raises exception, because `xpaths` changes a lot.
- **Milestone**: Install a Google Chrome browser into the Job Clusters to do the scraper for us.
- **Main Audience**: Other interested engineers and I.
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

# Design Considerations

## Datasources
- Website FIPE
- Volume of the data is not so big. 
## Data Ingestion
We will create scraper/extraction functions to extract the reference month, brands, models, manufacturing year and kind of fuel.
For each reference month I will have one brand, within the brand, many models for different manufacturing year and fuel.
## Data Storage
## Data Processing
## Data Consumption
## Data Operation ( DataOps )
## Data Governance
## Data Security

# Design Principles
> Design Principles help to define the common rules & standards that need to be followed while implementing the system. These principles can help build a common understanding across various teams using the central data platform.
# Tech Solution
