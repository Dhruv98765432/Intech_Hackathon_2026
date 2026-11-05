# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0ad58971-cd5a-47ab-9b81-7e2eeba75f31",
# META       "default_lakehouse_name": "raw_data",
# META       "default_lakehouse_workspace_id": "a740cbb5-cee1-41d2-bd69-e6c4571cd003",
# META       "known_lakehouses": [
# META         {
# META           "id": "0ad58971-cd5a-47ab-9b81-7e2eeba75f31"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np

from datetime import datetime, date


# ============================================================
# 1. CONFIGURATION
# ============================================================

excel_file_path = "/lakehouse/default/Files/Revenue_Synthetic.xlsx"

sheet_name = "Sheet1"

target_table = "revenue"


# ============================================================
# 2. READ EXCEL FILE
# ============================================================

print("Reading Excel file...")

pdf = pd.read_excel(
    excel_file_path,
    sheet_name=sheet_name,
    engine="openpyxl"
)

# Clean column names
pdf.columns = pdf.columns.astype(str).str.strip()

# Check duplicate columns
if pdf.columns.duplicated().any():
    raise ValueError("Duplicate column names found.")

print("Total Records:", len(pdf))
print("Total Columns:", len(pdf.columns))


# ============================================================
# 3. AUTOMATIC SCHEMA DETECTION
# ============================================================

print("\nDetecting Data Types...")

spark_fields = []

# Identifier-related keywords
identifier_keywords = [
    "invoice_no",
    "invoice_number",
    "sales_order_no",
    "sales_order_number",
    "purchase_order_no",
    "purchase_order_number",
    "customer_id",
    "vendor_id",
    "product_id",
    "sku",
    "account_no",
    "account_number",
    "reference_no",
    "reference_number",
    "id",
    "code"
]

for column in pdf.columns:

    series = pdf[column].dropna()

    col_lower = column.lower().strip()

    # --------------------------------------------------------
    # Identifier columns -> StringType
    # --------------------------------------------------------

    is_identifier = (
        col_lower in identifier_keywords
        or col_lower.endswith("_id")
        or col_lower.endswith("_no")
        or col_lower.endswith("_number")
        or col_lower.endswith("_code")
    )

    if is_identifier:

        spark_type = StringType()

    # --------------------------------------------------------
    # All NULL columns -> StringType
    # --------------------------------------------------------

    elif len(series) == 0:

        spark_type = StringType()

    # --------------------------------------------------------
    # Boolean columns
    # --------------------------------------------------------

    elif pd.api.types.is_bool_dtype(pdf[column]):

        spark_type = BooleanType()

    # --------------------------------------------------------
    # Date / Timestamp columns
    # --------------------------------------------------------

    elif pd.api.types.is_datetime64_any_dtype(pdf[column]):

        spark_type = TimestampType()

    # --------------------------------------------------------
    # Integer columns
    # --------------------------------------------------------

    elif pd.api.types.is_integer_dtype(pdf[column]):

        spark_type = LongType()

    # --------------------------------------------------------
    # Float columns
    # --------------------------------------------------------

    elif pd.api.types.is_float_dtype(pdf[column]):

        spark_type = DoubleType()

    # --------------------------------------------------------
    # Default -> StringType
    # --------------------------------------------------------

    else:

        spark_type = StringType()

    spark_fields.append(
        StructField(
            column,
            spark_type,
            True
        )
    )

    print(f"{column} -> {spark_type.simpleString()}")


# ============================================================
# 4. CREATE EXPLICIT SPARK SCHEMA
# ============================================================

schema = StructType(spark_fields)

print("\nDetected Spark Schema:")
print(schema)


# ============================================================
# 5. PREPARE PANDAS DATA FOR SPARK
# ============================================================

print("\nConverting Pandas values to Python native types...")

from datetime import datetime, date
import pandas as pd
import numpy as np


def convert_value(value, spark_type):

    # Handle NULL / NaN / NaT
    if pd.isna(value):
        return None

    # StringType
    if isinstance(spark_type, StringType):
        return str(value)

    # TimestampType
    elif isinstance(spark_type, TimestampType):

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        elif isinstance(value, datetime):
            return value

        elif isinstance(value, date):
            return datetime.combine(value, datetime.min.time())

        else:
            return pd.to_datetime(value).to_pydatetime()

    # DateType
    elif isinstance(spark_type, DateType):

        if isinstance(value, pd.Timestamp):
            return value.date()

        elif isinstance(value, datetime):
            return value.date()

        elif isinstance(value, date):
            return value

        else:
            return pd.to_datetime(value).date()

    # BooleanType
    elif isinstance(spark_type, BooleanType):
        return bool(value)

    # LongType
    elif isinstance(spark_type, LongType):
        return int(value)

    # IntegerType
    elif isinstance(spark_type, IntegerType):
        return int(value)

    # DoubleType
    elif isinstance(spark_type, DoubleType):
        return float(value)

    # FloatType
    elif isinstance(spark_type, FloatType):
        return float(value)

    # DecimalType
    elif isinstance(spark_type, DecimalType):
        return decimal.Decimal(str(value))

    # NumPy scalar conversion
    elif isinstance(value, np.generic):
        return value.item()

    return value


# Build native Python records using the detected schema
records = []

for row in pdf.itertuples(index=False, name=None):

    converted_row = tuple(
        convert_value(value, field.dataType)
        for value, field in zip(row, schema.fields)
    )

    records.append(converted_row)


print("Python native type conversion completed.")


# ============================================================
# 6. CREATE SPARK DATAFRAME
# ============================================================

print("\nCreating Spark DataFrame...")

df = spark.createDataFrame(
    records,
    schema=schema,
    verifySchema=True
)

print("\nSpark DataFrame Created Successfully.")

df.printSchema()

display(df.limit(10))


# ============================================================
# 6. CREATE SPARK DATAFRAME
# ============================================================

print("\nCreating Spark DataFrame...")

# Convert Pandas DataFrame into native Python records
records = pdf.to_dict(orient="records")

# Create Spark DataFrame with explicit schema
df = spark.createDataFrame(
    records,
    schema=schema,
    verifySchema=True
)

print("\nSpark DataFrame Created Successfully.")

df.printSchema()


# ============================================================
# 7. WRITE DATA INTO LAKEHOUSE
# ============================================================

print(f"\nWriting data into Lakehouse: {target_table}")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"SUCCESS: {target_table} overwritten successfully.")


# ============================================================
# 8. VALIDATE FINAL TABLE
# ============================================================

print("\nValidating Final Lakehouse Table...")

final_df = spark.read.table(target_table)

print("\nFinal Lakehouse Schema:")

final_df.printSchema()

source_count = len(pdf)

loaded_count = final_df.count()

print("\nSource Records:", source_count)

print("Loaded Records:", loaded_count)

if source_count == loaded_count:

    print("VALIDATION SUCCESS: All records loaded.")

else:

    print("WARNING: Source and target record counts do not match.")


# ============================================================
# 9. DISPLAY FINAL DATA
# ============================================================

display(final_df.limit(10))

print("\nETL Completed Successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURATION: ADD DATE COLUMNS FOR EACH TABLE
# ============================================================

table_date_columns = {
    "revenue": [
        "Invoice_Creation_Date",
        "PO_Date",
    ],

    "sales": [
        "InvoiceCreationDate",
        "InvoiceDate",
        "PODate",
        "MarketingRequirementDate",
        "PlantRequirementDate",
        "InvoiceDateForCalendar",
    ]
}

# ============================================================
# 2. PROCESS TABLES
# ============================================================

for table_name, date_columns in table_date_columns.items():

    print(f"\nProcessing table: {table_name}")

    try:
        # Read existing Lakehouse Delta table
        df = spark.read.table(table_name)

        # Check available columns
        existing_columns = df.columns

        # Convert specified columns to DATE
        for column_name in date_columns:

            if column_name in existing_columns:

                df = df.withColumn(
                    column_name,
                    F.to_date(F.col(column_name))
                )

                print(f"  Converted: {column_name} -> DATE")

            else:
                print(f"  WARNING: Column '{column_name}' not found.")

        # ====================================================
        # 3. OVERWRITE EXISTING LAKEHOUSE TABLE
        # ====================================================

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

        print(f"SUCCESS: {table_name} updated successfully.")

        # ====================================================
        # 4. VALIDATE UPDATED SCHEMA
        # ====================================================

        print(f"\nUpdated schema for {table_name}:")
        spark.read.table(table_name).printSchema()

    except Exception as e:

        print(f"ERROR processing {table_name}: {str(e)}")

print("\nAll table processing completed.")

please upload to lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# ============================================================
# 5. PREPARE PANDAS DATA FOR SPARK
# ============================================================

print("\nConverting Pandas values to Python native types...")

from datetime import datetime, date
import pandas as pd
import numpy as np


def convert_value(value, spark_type):

    # Handle NULL / NaN / NaT
    if pd.isna(value):
        return None

    # StringType
    if isinstance(spark_type, StringType):
        return str(value)

    # TimestampType
    elif isinstance(spark_type, TimestampType):

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        elif isinstance(value, datetime):
            return value

        elif isinstance(value, date):
            return datetime.combine(value, datetime.min.time())

        else:
            return pd.to_datetime(value).to_pydatetime()

    # DateType
    elif isinstance(spark_type, DateType):

        if isinstance(value, pd.Timestamp):
            return value.date()

        elif isinstance(value, datetime):
            return value.date()

        elif isinstance(value, date):
            return value

        else:
            return pd.to_datetime(value).date()

    # BooleanType
    elif isinstance(spark_type, BooleanType):
        return bool(value)

    # LongType
    elif isinstance(spark_type, LongType):
        return int(value)

    # IntegerType
    elif isinstance(spark_type, IntegerType):
        return int(value)

    # DoubleType
    elif isinstance(spark_type, DoubleType):
        return float(value)

    # FloatType
    elif isinstance(spark_type, FloatType):
        return float(value)

    # DecimalType
    elif isinstance(spark_type, DecimalType):
        return decimal.Decimal(str(value))

    # NumPy scalar conversion
    elif isinstance(value, np.generic):
        return value.item()

    return value


# Build native Python records using the detected schema
records = []

for row in pdf.itertuples(index=False, name=None):

    converted_row = tuple(
        convert_value(value, field.dataType)
        for value, field in zip(row, schema.fields)
    )

    records.append(converted_row)


print("Python native type conversion completed.")


# ============================================================
# 6. CREATE SPARK DATAFRAME
# ============================================================

print("\nCreating Spark DataFrame...")

df = spark.createDataFrame(
    records,
    schema=schema,
    verifySchema=True
)

print("\nSpark DataFrame Created Successfully.")

df.printSchema()

display(df.limit(10))


# ============================================================
# 6. CREATE SPARK DATAFRAME
# ============================================================

print("\nCreating Spark DataFrame...")

# Convert Pandas DataFrame into native Python records
records = pdf.to_dict(orient="records")

# Create Spark DataFrame with explicit schema
df = spark.createDataFrame(
    records,
    schema=schema,
    verifySchema=True
)

print("\nSpark DataFrame Created Successfully.")

df.printSchema()


# ============================================================
# 7. WRITE DATA INTO LAKEHOUSE
# ============================================================

print(f"\nWriting data into Lakehouse: {target_table}")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"SUCCESS: {target_table} overwritten successfully.")


# ============================================================
# 8. VALIDATE FINAL TABLE
# ============================================================

print("\nValidating Final Lakehouse Table...")

final_df = spark.read.table(target_table)

print("\nFinal Lakehouse Schema:")

final_df.printSchema()

source_count = len(pdf)

loaded_count = final_df.count()

print("\nSource Records:", source_count)

print("Loaded Records:", loaded_count)

if source_count == loaded_count:

    print("VALIDATION SUCCESS: All records loaded.")

else:

    print("WARNING: Source and target record counts do not match.")


# ============================================================
# 9. DISPLAY FINAL DATA
# ============================================================

display(final_df.limit(10))

print("\nETL Completed Successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
import pandas as pd
import numpy as np
import decimal

from datetime import datetime, date, time


# ============================================================
# 1. CONFIGURATION
# ============================================================

excel_file_path = "/lakehouse/default/Files/Revenue_Synthetic.xlsx"

sheet_name = "Sheet1"

target_table = "revenue"


# ============================================================
# 2. READ EXCEL FILE
# ============================================================

print("Reading Excel file...")

pdf = pd.read_excel(
    excel_file_path,
    sheet_name=sheet_name,
    engine="openpyxl"
)

pdf.columns = pdf.columns.astype(str).str.strip()

if pdf.columns.duplicated().any():
    raise ValueError("Duplicate column names found.")

print("Total Records:", len(pdf))
print("Total Columns:", len(pdf.columns))


# ============================================================
# 3. AUTOMATIC SCHEMA DETECTION
# ============================================================

print("\nDetecting Data Types...")

spark_fields = []

identifier_keywords = [
    "invoice_no",
    "invoice_number",
    "sales_order_no",
    "sales_order_number",
    "purchase_order_no",
    "purchase_order_number",
    "customer_id",
    "vendor_id",
    "product_id",
    "sku",
    "account_no",
    "account_number",
    "reference_no",
    "reference_number"
]

for column in pdf.columns:

    series = pdf[column].dropna()
    col_lower = column.lower().strip()

    is_identifier = (
        col_lower in identifier_keywords
        or col_lower.endswith("_id")
        or col_lower.endswith("_no")
        or col_lower.endswith("_number")
        or col_lower.endswith("_code")
    )

    if is_identifier:
        spark_type = StringType()

    elif len(series) == 0:
        spark_type = StringType()

    elif pd.api.types.is_bool_dtype(pdf[column]):
        spark_type = BooleanType()

    elif pd.api.types.is_datetime64_any_dtype(pdf[column]):
        spark_type = TimestampType()

    elif pd.api.types.is_integer_dtype(pdf[column]):
        spark_type = LongType()

    elif pd.api.types.is_float_dtype(pdf[column]):
        spark_type = DoubleType()

    else:
        spark_type = StringType()

    spark_fields.append(
        StructField(column, spark_type, True)
    )

    print(f"{column} -> {spark_type.simpleString()}")


# ============================================================
# 4. CREATE SPARK SCHEMA
# ============================================================

schema = StructType(spark_fields)

print("\nSpark Schema:")
print(schema.simpleString())


# ============================================================
# 5. CONVERT VALUES TO PYTHON NATIVE TYPES
# ============================================================

print("\nPreparing Python records...")


def is_null(value):

    if value is None:
        return True

    if value is pd.NaT:
        return True

    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return True

    return False


def convert_value(value, spark_type):

    # NULL handling
    if is_null(value):
        return None

    # Convert Pandas / NumPy datetime values
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        value = pd.Timestamp(value).to_pydatetime()

    # StringType
    if isinstance(spark_type, StringType):
        return str(value)

    # TimestampType
    elif isinstance(spark_type, TimestampType):

        if isinstance(value, datetime):
            return value

        elif isinstance(value, date):
            return datetime.combine(value, time.min)

        else:
            return pd.to_datetime(value).to_pydatetime()

    # DateType
    elif isinstance(spark_type, DateType):

        if isinstance(value, datetime):
            return value.date()

        elif isinstance(value, date):
            return value

        else:
            return pd.to_datetime(value).date()

    # BooleanType
    elif isinstance(spark_type, BooleanType):
        return bool(value)

    # LongType
    elif isinstance(spark_type, LongType):
        return int(value)

    # IntegerType
    elif isinstance(spark_type, IntegerType):
        return int(value)

    # DoubleType
    elif isinstance(spark_type, DoubleType):
        return float(value)

    # FloatType
    elif isinstance(spark_type, FloatType):
        return float(value)

    # DecimalType
    elif isinstance(spark_type, DecimalType):
        return decimal.Decimal(str(value))

    # Other NumPy values
    elif isinstance(value, np.generic):
        return value.item()

    return value


# Convert every row into a native Python tuple
records = []

for row in pdf.itertuples(index=False, name=None):

    converted_row = []

    for value, field in zip(row, schema.fields):

        converted_value = convert_value(
            value,
            field.dataType
        )

        converted_row.append(converted_value)

    records.append(tuple(converted_row))


print("Total Python Records:", len(records))


# ============================================================
# 6. VALIDATE CONVERTED DATATYPES
# ============================================================

print("\nValidating converted values...")

for field_index, field in enumerate(schema.fields):

    if isinstance(field.dataType, TimestampType):

        sample_value = next(
            (
                row[field_index]
                for row in records
                if row[field_index] is not None
            ),
            None
        )

        if sample_value is not None:

            print(
                f"{field.name}: "
                f"{sample_value} -> "
                f"{type(sample_value)}"
            )

            if not isinstance(sample_value, datetime):
                raise TypeError(
                    f"{field.name} is not converted to Python datetime."
                )


# ============================================================
# 7. CREATE SPARK DATAFRAME
# ============================================================

print("\nCreating Spark DataFrame...")

df = spark.createDataFrame(
    records,
    schema=schema,
    verifySchema=True
)

print("\nSpark DataFrame Created Successfully!")

df.printSchema()


# ============================================================
# 8. OVERWRITE LAKEHOUSE DELTA TABLE
# ============================================================

print(f"\nWriting data into Lakehouse: {target_table}")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"SUCCESS: {target_table} overwritten successfully.")


# ============================================================
# 9. VALIDATE FINAL TABLE
# ============================================================

print("\nValidating final Lakehouse table...")

final_df = spark.read.table(target_table)

print("\nFinal Table Schema:")
final_df.printSchema()

source_count = len(records)
loaded_count = final_df.count()

print("\nSource Records:", source_count)
print("Loaded Records:", loaded_count)

if source_count == loaded_count:
    print("VALIDATION SUCCESS: All records loaded.")
else:
    print("WARNING: Source and target counts do not match.")


# ============================================================
# 10. DISPLAY FINAL DATA
# ============================================================

display(final_df.limit(10))

print("\nETL Completed Successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
