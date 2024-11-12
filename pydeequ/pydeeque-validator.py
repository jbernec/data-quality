# Databricks notebook source
# https://www.youtube.com/watch?v=StmNhkChgRs

# COMMAND ----------

pip install pydeequ==1.1.1

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
data = [
            (1, "Alice", "Smith", 25, "alice.smith@email.com", "666-666-7777", "Female"),
            (2, "Bob", "Brown", 30, "bob.brown@email.com", "888-888-8888", "Male"),
            (3, "Carol", "Johnson", 28, "carol.johnson", "999-999-9999", "Female"),
            (4, "Dave", "Wilson", 0, "dave.wilson@email.com", "+1234567893", "Male")
        ]
        
        # Schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("gender", StringType(), True)
])
# Crea DataFrame
df_demo = spark.createDataFrame(data, schema) 
df_demo.display()

# COMMAND ----------

# MAGIC %env SPARK_VERSION=3.2

# COMMAND ----------

from pydeequ.analyzers import *
import pydeequ
from pyspark.sql import SparkSession, DataFrame

spark_conf = (
    SparkSession
        .builder
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
)

analysisResult = AnalysisRunner(spark_conf) \
                    .onData(df_demo) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("id")) \
                    .addAnalyzer(Distinctness("first_name")) \
                    .addAnalyzer(Uniqueness(["first_name"])) \
                    .addAnalyzer(Uniqueness(["last_name", "email"])) \
                    .addAnalyzer(Compliance("gender", "gender in ('Female','Male')")) \
                    .addAnalyzer(Mean("age")) \
                    .addAnalyzer(Sum("id")) \
                    .addAnalyzer(Maximum("age")) \
                    .run()
                   
                    
analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark_conf, analysisResult)
analysisResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasSize(assertion, hint=None)
# MAGIC
# MAGIC Creates a constraint that calculates the data frame size and runs the assertion on it.
# MAGIC

# COMMAND ----------

from pydeequ.checks import *
from pydeequ.verification import *
check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasSize(lambda x: x == 4))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark_conf, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC isComplete(column, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts on a column completion

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.isComplete('id'))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC areComplete(columns, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts completion in combined set of columns.
# MAGIC

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.areComplete(['id','age']))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasCompleteness(column, assertion, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts column completion. Uses the given history selection strategy to retrieve
# MAGIC historical completeness values on this column from the history provider.
# MAGIC

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasCompleteness('id',lambda x: x >= 0.7))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasUniqueness(columns, assertion, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts any uniqueness in a single or combined set of key

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasUniqueness(['id','age'],lambda x : x >= 0.7))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasMin(column, assertion, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts on the minimum of a column. The column is contains either a long, int or
# MAGIC float datatype.

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasMin('age',lambda x : x >= 0))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasMax(column, assertion, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts on the maximum of the column. The column contains either a long, int or
# MAGIC float datatype.
# MAGIC

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasMax('age',lambda x : x >= 20))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasMean(column, assertion, hint=None)
# MAGIC
# MAGIC Creates a constraint that asserts on the mean of the column
# MAGIC

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasMean('age',lambda x : x == 30))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC isPositive(column, assertion=None, hint=None)
# MAGIC
# MAGIC Creates a constraint which asserts that a column contains no negative values and is greater than 0.

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.isPositive('age')\
                 .isPositive('id'))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.isPositive('age',lambda x : x ==0.1))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC isNonNegative(column, assertion=None, hint=None)
# MAGIC
# MAGIC Creates a constraint which asserts that a column contains no negative values.

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.isNonNegative('age')\
                 .isNonNegative('id'))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC isContainedIn(column, allowed_values, assertion=None, hint=None)
# MAGIC
# MAGIC Asserts that every non-null value in a column is contained in a set of predefined values

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.isContainedIn("gender", ["Female", "Male"]))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasPattern(column, pattern, assertion=None, name=None, hint=None)
# MAGIC
# MAGIC Checks for pattern compliance. Given a column name and a regular expression, defines a Check on the
# MAGIC average compliance of the column’s values to the regular expression.

# COMMAND ----------

check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasPattern("email",r"a*",lambda x:x>=0.1))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC hasDataType(column, datatype: ConstrainableDataTypes, assertion=None, hint=None)
# MAGIC
# MAGIC Check to run against the fraction of rows that conform to the given data type.

# COMMAND ----------

from pydeequ.checks import *
from pydeequ.verification import *
check = Check(spark_conf, CheckLevel.Warning, "Demo")
checkResult = VerificationSuite(spark_conf) \
 .onData(df_demo) \
 .addCheck(check.hasDataType("id",ConstrainableDataTypes.String))\
 .run()
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

import pydeequ
import json
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import *
from pyspark.sql import SparkSession, DataFrame
from typing import  Dict, List
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# os.environ['SPARK_VERSION'] = '3.2'
spark_conf = (
    SparkSession
        .builder
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
)

class Metadata:

    def __init__(self, rule_id: str, cols: str,dimension: str):
        self.rule_id = rule_id
        self.cols = cols
        self.dimension = dimension
        self.datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


class PyDQChecker:
    
    def check_completeness(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        cols = params["input_attributes"]
        threshold = params.get("threshold") or 1.0
        check = Check(spark_conf, strategy, metadata.to_json())
        check.haveCompleteness(cols, lambda x: x == threshold)
        return check
     
    
    def check_uniquiness(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        cols = params["input_attributes"]
        threshold = params.get("threshold") or 1.0
        check = Check(spark_conf, strategy, metadata.to_json())
        check.hasUniqueness(cols, lambda x: x == threshold)
        return check
            
    
    def check_pattern(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        pattern = params["pattern"]
        cols = params["input_attributes"]
        threshold = params.get("threshold") or 1.0
        check = Check(spark_conf, strategy, metadata.to_json())
        for col in cols:
            check.hasPattern(col, pattern, lambda x: x >= threshold)
        return check
    
    
    def check_email(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        cols = params["input_attributes"]
        threshold = params.get("threshold") or 1.0
        check = Check(spark_conf, strategy, metadata.to_json())
        for col in cols:
            check.containsEmail(col, lambda x: x == threshold)
        return check
    
    def check_isContainedIn(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        cols = params["input_attributes"]
        threshold = params.get("threshold") or 1.0
        contained_values = params.get("contained_values") or None
        check = Check(spark_conf, strategy, metadata.to_json())
        for col in cols:
            check.isContainedIn(col, contained_values)
        return check
    
    def check_isPositive(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        cols = params["input_attributes"]
        threshold = params.get("threshold") or 1.0
        check = Check(spark_conf, strategy, metadata.to_json())
        for col in cols:
            check.isPositive(col,lambda x : x >= threshold)
        return check
    
    def check_hasDataType(params: Dict) -> None:
        metadata = Metadata(params["rule_id"],params["input_attributes"],params["dimension"])
        strategy =  params["strategy"]
        cols = params["input_attributes"]
        datatype = params.get("datatype") or None
        check = Check(spark_conf, strategy, metadata.to_json())
        for col in cols:
            check.hasDataType(col,datatype)
        return check
    
    def run(df: DataFrame, checks: List):
        suite = VerificationSuite(spark_conf).onData(df)
        for check in checks:
            suite.addCheck(check)
        check_result = suite.run()
        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
        return checkResult_df


class DataQualityRules:
    def get_rules(self):
        return [
        {
            "rule_id": "COM.RULE.001",
            "input_attributes": ["age"],
            "strategy": CheckLevel.Warning,
            "pattern": "",
            "operation": PyDQChecker.check_completeness,
            "dimension": "Completeness",
            "datetime": datetime.now()
           
        },
        {
            "rule_id": "UNQ.RULE.001",
            "input_attributes": ["last_name"],
            "strategy": CheckLevel.Warning,
            "pattern": "",
            "operation": PyDQChecker.check_uniquiness,
            "dimension": "Uniqueness",
            "datetime": datetime.now()
        },
        {
            "rule_id": "VAL.RULE.001",
            "input_attributes": ["first_name", "last_name"],
            "strategy": CheckLevel.Warning,
            "pattern": "^[a-zA-Z]+$",
            "operation": PyDQChecker.check_pattern,
            "dimension": "Validity",
            "datetime": datetime.now()
        },
        {
            "rule_id": "VAL.RULE.002",
            "input_attributes": ["phone_number"],
            "strategy": CheckLevel.Warning,
            "pattern": "^(\+?\d{1,3}( )?)?[- .]?((\(\d{3}\))|\d{3})[- .]?\d{3}[- .]?\d{4}$",
            "operation": PyDQChecker.check_pattern,
            "dimension": "Validity",
            "threshold": 0.7,
            "datetime": datetime.now()
        },
        {
            "rule_id":  "VAL.RULE.003",
            "input_attributes": ["email"],
            "strategy": CheckLevel.Warning,
            "pattern": "",
            "operation": PyDQChecker.check_email,
            "dimension": "Validity",
            "threshold": 0.5,
            "datetime": datetime.now()
        },
        {
            "rule_id":  "VAL.RULE.004",
            "input_attributes": ["gender"],
            "strategy": CheckLevel.Warning,
            "pattern": "",
            "operation": PyDQChecker.check_isContainedIn,
            "dimension": "Validity",
            "datetime": datetime.now(),
            "contained_values": ["Female", "Male","Other"],
        },
        {
            "rule_id":  "POS.RULE.001",
            "input_attributes": ["id"],
            "strategy": CheckLevel.Warning,
            "pattern": "",
            "operation": PyDQChecker.check_isPositive,
            "dimension": "Positiveness",
            "datetime": datetime.now()
        },
        {
            "rule_id":  "DT.RULE.001",
            "input_attributes": ["first_name"],
            "strategy": CheckLevel.Warning,
            "pattern": "",
            "operation": PyDQChecker.check_hasDataType,
            "dimension": "DataTypeValidation",
            "datetime": datetime.now(),
            "datatype": ConstrainableDataTypes.Integral
        }
    ]



class DQValidator:

    def __init__(self, catalog: str, schema: str, table_name: str):
        self.catalog = catalog
        self.schema = schema
        self.table_name = table_name
        self.report_suffix = "data_quality_report"
        self.metadata_schema = StructType([
            StructField("rule_id", StringType(), True),
            StructField("cols", StringType(), True),
            StructField("dimension", StringType(), True),
            StructField("datetime", TimestampType(), True)
        ])

    def execute(self, df_to_validate: DataFrame = None) -> bool:
        dq_rules = DataQualityRules()
        df_to_validate = df_demo
        dq_config = dq_rules.get_rules()
        checks = []
        for dqc in dq_config:
            print('dqc....',dqc)
            dq_rule_operation = dqc["operation"]
            dqc.pop("operation")
            checks.append(dq_rule_operation(dqc))
        
        res_df = PyDQChecker.run(df_to_validate, checks)
        res_df.display()
        res_df = res_df.withColumn("metadata", from_json(col("check"), self.metadata_schema))
        # Expanding the JSON data into separate columns
        res_df = res_df.select("*","metadata.*").drop('check')
        res_df.display()
        res_df.write.mode('append').saveAsTable(f'{self.table_name}_{self.report_suffix}')
        

        return True


if __name__ == "__main__":
        # Data
        data = [
            (1, "Alice", "Smith", 25, "alice.smith@email.com", "666-666-7777", "Female"),
            (2, "Bob", "Brown", 30, "bob.brown@email.com", "888-888-8888", "Male"),
            (3, "Carol", "Johnson", 28, "carol.johnson", "999-999-9999", "Female"),
            (-5, "Dave", "Wilson", 35, "dave.wilson@email.com", "+1234567893", "Male")
        ]
        
        # Schema
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("gender", StringType(), True)
        ])

# Create DataFrame
        df_demo = spark.createDataFrame(data, schema) 
        validation_catalog = 'hive_metastore'
        validation_schema = 'default'
        validation_table_name = 'data'
        dq_validator = DQValidator(validation_catalog, validation_schema, validation_table_name)
        dq_validator.execute(df_demo)
