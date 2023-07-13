| Start Date |
| ---------- |
| 2023-06-09 |

# Overview
Build a pipeline to extract, load and transform the data. According the data moves for each layer, the quality will increase.

We are going to scrape data from the website [FIPE](https://veiculos.fipe.org.br/) which holds Average Vehicle Price.

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
- **Milestone**: Install a Google Chrome browser into the Job Clusters to do the scrape for us. It will run as `--headless`
- **Main Audience**: Other interested engineers and I.

# Requirements
1. Run the pipeline once a month.
2. Use [Delta Lake](https://delta.io/) as Storage framework.
3. Use Functional Programming & OOP for some cases.
4. [pytest](https://docs.pytest.org/en/7.3.x/) as test framework

# Design Considerations

## Datasources
- Website [FIPE](https://veiculos.fipe.org.br/)
- Volume of the data is not so big.
## Data Ingestion
Functions for extraction will be on `fipe/elt/extract/utils.py`

We will create scraper/extraction functions to extract the *reference months*, and the complete table containing information about the vehicles.

## Data Storage
Functions for loading the tables will be on `fipe/elt/load/utils.py`

We will storage it on `dbfs` to mimic a mount point for ADLS Gen2 Containers.
- `mnt/bronze`
- `mnt/silver`
- `mnt/gold`

## Data Processing
We will use Pyspark to process the data.

## Data Consumption
Create a token on Databricks Workspace to consume the Data on DBFS
We will use Import Mode on Power BI Desktop to create the report.


# Design Principles
> Design Principles help to define the common rules & standards that need to be followed while implementing the system. These principles can help build a common understanding across various teams using the central data platform.

## Costs
For being a small workload we are going to use **Single Node**  cluster `Standard_DS3_v2` with 4vCores

## Operational
Log almost everything important for us. Especially Browser, Buttons the automation in general.

We will use a `StreamHandler` to create our Logger to log out info to our `stdout`

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
However I can have situations where, for a specific *reference month* I don't have a particular *brand*,*model* or *manufacturing year - fuel*.

The flow will be ---> Extract ALL *reference months* available and save it as Delta.

Then, we will read the table *reference months* and iterate over them to add into the box, get all *brands*, *models* and *manufacturing year - fuel* available for that *reference month* in the context.

There is a function to extract the HTML table over the tag `<tbody> Table </tbody>` to return as Dict and appending into a List.

When ALL *brands*, *models* and *manufacturing year - fuel* are done for a SPECIFIC *reference month* , will be generated a `List[Dict]`, transform it to a PySpark DataFrame and save it on our `mnt/bronze`.

>The workflow stated above will run until all data be scraped.
The final result on `Bronze Layer` will be a Delta Table partitioned by *reference months* and *brands*.

## Workflow
- Task 1:
  1. Install Google Chrome in the Job Cluster.
- Task 2:
  1. Open Browser
  2. Extract all *reference months*
  3. Save them as Delta Tables
  4. Close Browser
- Task 3:
   1. Open Browser
   2. Read *reference months* and iterate over them to extract *brands*,*models*, *manufacturing year - fuel*
   3. After finishing one *reference month* we will save as Delta Table partitioned by *reference month*
   4. Repeat the Cycle
- Task 4:
  1. Read the tables on `Bronze` apply data cleansing, validation, add new columns and save it as Delta Table into `Silver Layer`
- Task 5:
   1. Create our facts and dimensions to save it on `Gold Layer`

## Data Assets
`Bronze Layer` we will have Delta Table `fipe_bronze` PARTITIONED BY *reference month* then the parquet files.
- Columns :
  - reference_month string
  - fipe_code string
  - brand string
  - model string
  - manufacturing year string
  - authentication string
  - query_date string
  - average_price string
> We are changing the name of the columns, because in portuguese, we have spaces, punctuation, then Delta Lake does not allow carry on saving the table.

`Silver Layer` basically the same Delta Table however with more columns.
- Containing First Date of the *reference month*
- Column REFERENCE YEAR
- Probably other info columns

`Gold Layer` create our fact and dimensions.

# Appendix

## Alternatives
There are tons of alternatives or stack we could apply instead Pyspark, Databricks.
One I would like to highlight is docker, polars, deltalake [delta-rs](https://github.com/delta-io/delta-rs) and [dagster](https://dagster.io/) to orchestrate our jobs as Data Assets.
