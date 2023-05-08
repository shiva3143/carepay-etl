# Data Engineer Code Challenge



​NOTE- 
1.AS THERE ARE NO GUIDELINES TO CLEAN AND AGGREGATE DATA I HAVE DONE SOME BASIC CLEANING AND AGGREGATION OPERATIONS
2.YOU NEED TO UPDATE CONFIG FILE WITH YOUR DETAILS

## Implementation Approach
 - The solution for the given use-case is completely implemented in python and pyspark programming approach.
 - Most core logics are configured in config.yaml file. The execution of the code is driven by configs.
 - Isolating the I/O & Data processing are isolated in different functions. main function in each module take care of I/O activities. while a separate function for each module implemented to achieve data processing.
 - Code level, There are 3 packages created in the root folder of the project
   - driver: This package contains the driver.py which acts as the starting point to the Spark Application.
   - processing: this package contains two modules. 1. pre_processing.py & 2. aggregation.py. Actual logics to pre-process the data and transform the data are implemented in these two modules.
   - utils: This package contains two modules. one is SparkSessionUtils.py module which is responsible to create SparkSession. common_utils.py module contains the reusable functions. These functions are utilized in processing to avoid the code redundancy and to increase the code Maintainability.
   - config.yaml: All properties used in code are maintained in config.yaml.Environment specific properties are under each environment i.e. local, dev, prod, etc.
   - requirements.txt: Holds all python packages required to execute this code.

## Data Exploration

NOTE- AS THERE ARE NO GUIDELINES TO CLEAN AND AGGREGATE DATA I HAVE DONE SOME BASIC CLEAN AND AGGREGATE OPERATIONS

 - Two processing modules involved. Data Pre-Processing & Aggregation
 - Data Pre-Processing
   - As part of data pre-processing, records with all null columns are dropped.
   - DATE_CREATED column is null in invoice,invoice_item and claims which is retrieved from treatment table
   - Negative PRODUCT_ID's removed and where amount is 0 that has been filtered out.
   - PROGRAM_ID field is fetched to treatment table from the CLAIMS TABLE
   - Though the pre-processed data is not partitioned, the partitioning functionality is already implemented and can be utilized by just adding one more config.

 - Aggregation
 Below aggregation logic is performed on the tables and final data is stored in redshift using SQLALCHEMY library (  I could have used pyspark redshift connector to create the tables in redshift but was facing issues with library version so did using SQLALCHEMY )
   - TOTAL INVOICE BY CLAIM STATUS
   - TOTAL CLAIMS,TOTAL AMOUNT, MAX AMOUNT, MIN AMOUNT AVG AMOUNT BY STATUS OF CLAIM




1.The tool choices. Why was tool x used in your system?
- Local Environment
   - Required tools
     - Java 8/11/13
	 - mysql-jdbc-connector
     - Python 3.7.3
	 - pyspark 2.13
     - PyCharm [Community edition would work]
	 - Docker container should be up and running
	 - SQLALCHEMY
	 - Redshift workgroup should be running to access the data with required permissions.
     - All python modules mentioned in requirements.txt [pip install -r .\requirements.txt]
     - Make sure to set up all these environment variables [SPARK_HOME, HADOOP_HOME, JAVA_HOME, PYSPARK_PYTHON]
     - Create the Python venv for this project.
     - Run from PyCharm by creating the configuration for driver.
         - env: [local]
         - config_location: [Optional considering the config file is in current working directory, If not need to pass location] otherwise absolute path has to be passed with config file name
	
	pyspark is used to pull the data from mysql using jdbc connector and for pre-processing ie, cleaning and aggregation , I have used pyspark because of in-memory computation and I could have used spark-redshift connector and write it to redshift but due to library version issues I have to use SQLALCHEMY
	to load the aggregated data to redshift.
	
2 Structuring of the data in the single source of truth system. Why does this structure lead to easy and low latency queries?
	I have seen that there are duplicate data present in multiple tables like, date_created,program_id,status,type etc which can be avoided and can be retrived by joining on common column but I have not removed them.
	Redshift is a aws managed warehouse system so I have prefered to store aggregated data in it.
	
3 Scalability and maintainability. We understand that after one day your system will not be production ready. What would change to your design if you have to bring it to production.
  ​- schema design should be modified and remove the duplicate columns
  - code can be written more generic
  - I have provided config details we need to update as per production server.
  - We can eliminate sqlalchemy and use pyspark for reading as well as writing.
  
   # spark submit command: spark-submit --master yarn --deployment-mode cluster --conf spark.pyspark.python="path to python installation"--driver-memory 2g --conf py-files "location to zipped python code [Current repo]" Driver.py dev /conf_path
   
- The above spark submit is basic command to run this code. However, we can tune by passing the executor memory, executor cores, num-executors, memory overhead etc.

Apologies for not able to complete the bonus challange.


Cheers,
Shivaji Mukkamala.