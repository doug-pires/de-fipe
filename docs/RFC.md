| Start Date |
| ---------- |
| 2023-06-09 |

# Overview
Build a pipeline to extract, load and transform the data. According the data moves for each layer, the quality will increase.
We are going to scrape data from the website [FIPE](https://veiculos.fipe.org.br/) which hold Average Vehicle Price.
One instance : I've bought a car. The brand is **DPIRRE** , model **Azx - 92** , Manufacturing Year is **2019** and the kind of fuel is **Gasoline**

| Brand  | Model    | Manufacturing Year | Fuel     | Price ( euros ) |
| ------ | -------- | ------------------ | -------- | --------------- |
| DPIREE | Azx - 92 | 2019               | Gasoline | 27.000          |

Over time my vehicle suffers depreciation or appreciation ( hard to happen ).

- **Goals**:
  - Deploy the pipeline into Databricks
  - Schedule it
  - Build a report on top of Delta tables
- **Non-Goals**: Pay attention if raises exception, because `xpaths` changes a lot.
- **Milestone**: Install a Google Chrome browser into the Job Clusters to do the scrape for us.
- **Main Audience**: Other interested engineers and I.
# Requirements ( High Level functional requirement )
1. Run the pipeline once a month.
2. Use [Delta Lake](https://delta.io/) as Storage framework.
3. Use Functional Programming & OOP for some cases.
4. [pytest](https://docs.pytest.org/en/7.3.x/) as test framework
- Databricks workspace
- Python scripts will run in Databricks as Workflows to make all the tasks.

# Design Considerations

## Datasources
- Website FIPE
- Volume of the data is not so big.
## Data Ingestion
- Functions for extraction will be on fipe/elt/extract/utils.py
We will create scraper/extraction functions to extract the reference month, brands, models, manufacturing year and kind of fuel.
For each reference month I will have one brand, within the brand, many models for different manufacturing year and fuel.
## Data Storage
- Functions for loading the tables will be on fipe/elt/load/utils.py
We will storage it on `dbfs` to mimic a mount point for ADLS Gen2 Containers.
- mnt/bronze
- mnt/silver
- mnt/gold
## Data Processing
We will use Pyspark to process the data.

## Data Consumption
Create a token on Databricks Workspace to consume the Data on DBFS
We will use Import Mode on Power BI Desktop to create the report.


# Design Principles
> Design Principles help to define the common rules & standards that need to be followed while implementing the system. These principles can help build a common understanding across various teams using the central data platform.

## Costs
For being a small workload we are going to use **Single Node** cluster.

## Operational
Log almost everything important for us. Especially Browser, Buttons the automation in general.

## Performance
Follow two **IMPORTANT PRINCIPLES** while we develop our pipeline:
- ETC ( Easy to Change )
- DRY

# Tech Solution
1. Set the local environment
 - [Poetry](https://python-poetry.org/) to manage our metadata project
 - Config Github
2. Install libraries we are going to use.
 - pyspark
 - delta-spark
 - pytest
 - [dbx](https://dbx.readthedocs.io/en/latest/)
 - selenium
 - beautifulsoup
 - among others
3. Create Configuration YML files for holding Dataframe names, schema and base paths.
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



## Source System Details
The website [FIPE](https://veiculos.fipe.org.br/) has its particularity.
One instance:
I can extract ALL *reference months* and *brands* with two automations, which is **OPEN BROWSER ---> CLICK THE BUTTON VEHICLES.**
However I can have situations where, for a specific *reference month* I dont have a particular *brand*,*model* or *manufacturing year - fuel*
The flow will be. Extract ALL *reference months* available and save it as Delta.
Then, we will read the table *reference months* and iterate over them to add into the box, get all *brands*, *models* and *manufacturing year - fuel* available. We will save the Delta Table partitioned by *reference months*.

Extract the *models* I need more automations, which is **ADD A SPECIFIC BRAND ---> EXTRACT ALL MODELS**
Extract the *manufacturing year - fuel* for a specific *brand* and *model* I need one more automation **ADD A SPECIFIC MODEL ---> EXTRACT ALL MANUFACTURING YEAR - FUEL**

## Workflow
- Task 1:
  1. Open Browser
  2. Extract all *reference months* and *brands*
  3. Save them as Delta Tables
  4. Close Browser
- Task 2:
   1. Open Browser
   2. Read *reference months* and *brands*
   3.
4.

## Data Assets
Once we are thinking about extraction
