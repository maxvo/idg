from attrs import define
from pyspark.sql import DataFrame
from pyspark.sql.functions import first
from pyspark.sql.types import DecimalType


@define
class DataQualityOutput:
    """Make transformations to dataframes before returning it."""
    df_profiling: DataFrame

    def transpose(self, df: DataFrame):
        return (df
                .groupBy("entity", "instance")
                .pivot("name")
                .agg(first("value")))

    def cast_decimal(self,
                     df: DataFrame,
                     column: str,
                     precision: int = 38,
                     scale: int = 4):
        return (df.withColumn(column,
                              df[column]
                              .cast(DecimalType(precision, scale))))

    def rename(self, df: DataFrame, old_name: str, new_name: str):
        return df.withColumnRenamed(old_name, new_name)

    def rename_profiling(self, df: DataFrame):
        df1 = self.rename(df, "instance", "nome_coluna")
        df2 = self.rename(df1, "ApproxQuantiles-0.25", "1_quartil")
        df3 = self.rename(df2, "ApproxQuantiles-0.5", "2_quartil")
        df4 = self.rename(df3, "ApproxQuantiles-0.75", "3_quartil")
        return self.rename(df4, "Completeness", "completude")

    def dataframe(self):
        profiling = self.cast_decimal(self.df_profiling, "value")
        profiling = self.transpose(profiling)
        self.rename_profiling(profiling).show(truncate=False)
        # self.df_profiling.show()
