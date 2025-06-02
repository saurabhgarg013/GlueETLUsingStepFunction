from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_date


def check_null(df: DataFrame, column_name: str) -> DataFrame:
    """
    Flags null or empty string values in the given column.
    """
    return df.withColumn(
        f"{column_name}_null_check_flag",
        when(col(column_name).isNull() | (col(column_name) == ""), 0).otherwise(1)
    )


def check_special_characters(df: DataFrame, column_name: str, allowed_pattern: str) -> DataFrame:
    """
    Flags values in the given column that do not match the allowed regex pattern.
    Example: allowed_pattern = '^[a-zA-Z ]+$'
    """
    return df.withColumn(
        f"{column_name}_char_check_flag",
        when(~col(column_name).rlike(allowed_pattern), 0).otherwise(lit(1))
    )


def check_date_format(df: DataFrame, column_name: str, date_format: str) -> DataFrame:
    """
    Validates that the given column matches the expected date format.
    Example: date_format = 'yyyy-MM-dd'
    """
    return df.withColumn(
        f"{column_name}_date_check_flag",
        when(to_date(col(column_name), date_format).isNull(), 0).otherwise(lit(1))
    )
