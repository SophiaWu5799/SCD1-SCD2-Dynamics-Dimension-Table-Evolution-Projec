# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table if exists de_001.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a new employee table in the de_001 database 
# MAGIC create table if not exists de_001.employee (
# MAGIC     emp_id INT, 
# MAGIC     fname STRING, 
# MAGIC     lname STRING, 
# MAGIC     salary INT, 
# MAGIC     dept_id INT
# MAGIC )
# MAGIC --  set it to ture to capture and process changes to a table's data over time
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# Define MySQL connection details
mysql_properties = {
    'jdbc_url': 'jdbc:mysql://database.ascendingdc.com:3306/de_001',
    'jdbc_driver': "com.mysql.jdbc.Driver",
    'user': 'sophiawu',
    'password': 'welcome',
    'dbtable': 'employee'
}

# COMMAND ----------

# use jdbc to connect to mysql and list all table in de_001 database

jdbc_url = 'jdbc:mysql://database.ascendingdc.com:3306/de_001'
user = 'student'
password = '1234abcd'
jdbc_driver = "com.mysql.jdbc.Driver"


db_name = 'de_001'
table_list = (
    spark.read.format("jdbc")
    .option("driver", jdbc_driver)
    .option("url", jdbc_url)
    .option("dbtable", "information_schema.tables")
    .option("user", user)
    .option("password", password)
    .load()
    .filter(f"table_schema = '{db_name}'")
    .select("table_name")
)

table_list.show()

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("fname", StringType(), True),
    StructField("lname", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept_id", IntegerType(), True)
])



# COMMAND ----------

# 2.Insert 10 rows into the table

data= [(1, 'John', 'Doe', 50000, 101),
        (2, 'Jane', 'Smith', 60000, 102),
        (3, 'Bob', 'Johnson', 55000, 101),
        (4, 'Mary', 'Jones', 70000, 103),
        (5, 'Mike', 'Davis', 65000, 102),
        (6, 'Emily', 'Wilson', 45000, 101),
        (7, 'David', 'Brown', 80000, 103),
        (8, 'Sarah', 'Miller', 75000, 102),
        (9, 'Kevin', 'Lee', 60000, 101),
        (10, 'Lisa', 'Taylor', 85000, 103)] 

# COMMAND ----------

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Write the DataFrame to MySQL to create the table
df.write.jdbc(url=mysql_properties['jdbc_url'], 
            table=mysql_properties['dbtable'], 
            mode="overwrite", 
            properties=mysql_properties)


# COMMAND ----------

employee_df = (spark.read
            .jdbc(url=mysql_properties["jdbc_url"],
            table=mysql_properties["dbtable"],
            properties=mysql_properties)
)

employee_df.printSchema()
display(employee_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a database in Delta Lake, with the same name as in MySQL.
# MAGIC CREATE DATABASE IF NOT EXISTS de_001

# COMMAND ----------


employee_df.createOrReplaceTempView("employee_mysql")


# COMMAND ----------

# merge two tables

def merge_into_scd1(target_table, source_table, primary_keys, columns):
    a = f'''
    merge into {target_table} as target
    using {source_table} as source
    '''
    b = f'''
    on 
    '''
    first_key = True
    for i in primary_keys:
        if first_key:
            b = b + f'source.{i} = target.{i}'
            first_key = False
        else:
            b = b + f' and source.{i} = target.{i}'
    c = '''
    when matched and 
    '''
    d = '('
    first_column = True
    for i in columns:
        if first_column:
            d = d + f'source.{i} <> target.{i}'
            first_column = False
        else:
            d = d + f' or source.{i} <> target.{i}'
    e = ')'
    f = '''
    then update set *
    when not matched then insert *
    when not matched by source then delete
    '''
    query = a+b+c+d+e+f
    print(query)
    spark.sql(query)




# COMMAND ----------

# -- Use spark.read.jdbc to read from MySQL table and merge into Databricks table employee_scd1
# -- de_001.employee(scd1)
# -- employee_mysql (data from mysql database)


target_table = 'de_001.employee'
source_table = 'employee_mysql'
primary_keys = ['emp_id']
columns =['fname', 'lname', 'salary', 'dept_id']
merge_into_scd1(target_table, source_table, primary_keys, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from de_001.employee;

# COMMAND ----------

last_version=spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
print(last_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history de_001.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS de_001.employee_scd2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE de_001.employee_scd2 (
# MAGIC   emp_id INT,
# MAGIC   fname STRING,
# MAGIC   lname STRING,
# MAGIC   salary INT,
# MAGIC   dept_id INT,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# --  Use employee_scd1 CDF as input, merge into Databricks table employee_scd2

update_cdf_df= spark.sql(f"select * from table_changes('de_001.employee', {last_version},{last_version})")
update_cdf_df.createOrReplaceTempView('update_cdf_view');



# COMMAND ----------


def merge_into_scd2(target_table, source_table, primary_keys, columns):
    a = f'''
    MERGE INTO {target_table} AS target
    USING 
    (SELECT * FROM {source_table} WHERE _change_type != 'update_preimage') AS source 
    ON 
    '''
    b = ''
    first_key = True
    for i in primary_keys:
        if first_key:
            b = b + f'source.{i} = target.{i}'
            first_key = False
        else:
            b = b + f' AND source.{i} = target.{i}'

    c = '''
    WHEN MATCHED AND source._change_type = 'delete'
    THEN 
      UPDATE SET
      target.end_date = CURRENT_TIMESTAMP,
      target.is_current = FALSE

    WHEN MATCHED AND source._change_type = 'update_postimage' AND target.is_current = TRUE
    THEN 
      UPDATE SET
      target.end_date = CURRENT_TIMESTAMP,
      target.is_current = FALSE
    '''

    d = '''
    WHEN NOT MATCHED AND source._change_type = 'insert'
    THEN 
      INSERT (
    '''
    first_column = True
    for i in columns:
        if first_column:
            d += i
            first_column = False
        else:
            d += ',' + i
    d = '''
    WHEN NOT MATCHED AND source._change_type = 'insert'
    THEN 
      INSERT (
    '''
    first_column = True
    for i in columns:
        if first_column:
            d += i
            first_column = False
        else:
            d += ',' + i
    d += '''
      ) VALUES (
    '''
    first_column = True
    for i in columns[:-3]:
        if first_column:
            d += f'source.{i}'
            first_column = False
        else:
            d += ',' + f'source.{i}'
    d += f''',
      CURRENT_TIMESTAMP,
      NULL,
      TRUE
      );
    '''
    e = f'''
    insert into {target_table}
    select {','.join(columns[:-3])}, CURRENT_TIMESTAMP, NULL, True 
    from {source_table}
    where _change_type = 'update_postimage';
    '''

    query = a + b + c + d
    print(query)
    spark.sql(query)
    spark.sql(e)


 


# COMMAND ----------


target_table = 'de_001.employee_scd2'
source_table = 'update_cdf_view'
primary_keys = ['emp_id']
columns = ['emp_id', 'fname', 'lname', 'salary', 'dept_id', 'start_date', 'end_date', 'is_current']
merge_into_scd2(target_table, source_table, primary_keys, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * 
# MAGIC from de_001.employee_scd2;

# COMMAND ----------

# --1. Update one row of data in MySQL table

import mysql.connector

# Connect to MySQL database
mydb = mysql.connector.connect(
  host="database.ascendingdc.com",
  port=3306,
  user="sophiawu",
  password="welcome",
  database="de_001"
)

# Create cursor object
mycursor = mydb.cursor()




# COMMAND ----------

#Insert one row, update one row and delete one row in MySQL table


# Insert one row
mycursor.execute("INSERT INTO employee (emp_id, fname, lname, salary, dept_id) VALUES (12, 'Jim', 'Wong', 4000, 101)")

# Update one row
mycursor.execute("UPDATE employee SET salary = 98000 WHERE emp_id = 5")

# Delete one row
mycursor.execute("DELETE FROM employee WHERE emp_id = 4")

# Commit changes to database
mydb.commit()

# Close cursor and database connection
mycursor.close()
mydb.close()





# COMMAND ----------

# Read the updated table from mysql into a DataFrame
updated_df = (spark.read
        .jdbc(url=mysql_properties["jdbc_url"],
          table=mysql_properties["dbtable"],
          properties=mysql_properties)
)

updated_df.printSchema()
display(updated_df)
updated_df.createOrReplaceTempView("employee_updated_mysql")




# COMMAND ----------

# merge updated mysql data to scd1 table 

target_table = 'de_001.employee'
source_table = 'employee_updated_mysql'
primary_keys = ['emp_id']
columns =['fname', 'lname', 'salary', 'dept_id']
merge_into_scd1(target_table, source_table, primary_keys, columns)


# COMMAND ----------

last_version=spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
print(last_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY de_001.employee;

# COMMAND ----------

# use cdf to update scd2 table by using merge

final_cdf_df= spark.sql(f"select * from table_changes('de_001.employee', {last_version},{last_version})")
final_cdf_df.createOrReplaceTempView('final_cdf_view');

# COMMAND ----------

target_table = 'de_001.employee_scd2'
source_table = 'final_cdf_view'
primary_keys = ['emp_id']
columns = ['emp_id', 'fname', 'lname', 'salary', 'dept_id', 'start_date','end_date', 'is_current']
merge_into_scd2(target_table, source_table, primary_keys, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from de_001.employee_scd2;
