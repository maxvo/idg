from attrs import define
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DecimalType
from pydeequ.analyzers import AnalysisRunner, AnalysisRunBuilder,\
    Completeness, AnalyzerContext, ApproxQuantiles, ApproxCountDistinct,\
    DataType, Distinctness, Maximum, Mean, Minimum,\
    StandardDeviation, Sum, Uniqueness


@define
class DataProfiler:
    spark: SparkSession
    df: DataFrame

    def columns_name_and_type(self, df: DataFrame):
        return dict(df.dtypes)

    def default(self, profile: AnalysisRunBuilder, column: str):
        (profile
         .addAnalyzer(Completeness(column))
         .addAnalyzer(ApproxCountDistinct(column))
         .addAnalyzer(Distinctness(column))
         .addAnalyzer(Maximum(column))
         .addAnalyzer(Minimum(column))
         .addAnalyzer(Mean(column))
         .addAnalyzer(StandardDeviation(column))
         .addAnalyzer(Sum(column))
         .addAnalyzer(Uniqueness([column]))
         .addAnalyzer(ApproxQuantiles(column, [0.25, 0.5, 0.75])))

    def precision(self,
                  df: DataFrame,
                  column: str,
                  precision: int = 38,
                  scale: int = 4):
        return df.withColumn(
            column,
            df["column"]
            .cast(DecimalType(precision, scale)))

    def run(self):
        profile = AnalysisRunner(self.spark).onData(self.df)

        #[self.default(profile, column) for column in self.df.columns]
        self.default(profile, "TMAX")

        results = profile.run()

        analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, results)
        analysisResult_df.createOrReplaceTempView("results")

        analysisResult_df = self.precision(analysisResult_df, "value")

        analysisResult_df.show(100, truncate=False)

        print(analysisResult_df.dtypes)
