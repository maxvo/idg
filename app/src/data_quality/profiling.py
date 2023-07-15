from attrs import define, field
from pyspark.sql import SparkSession, DataFrame
from pydeequ.analyzers import AnalysisRunner,\
    Completeness, AnalyzerContext, ApproxQuantiles, CountDistinct,\
    Distinctness, Maximum, Mean, Minimum,\
    StandardDeviation, Sum, Uniqueness


@define
class DataProfiler:
    """Uses PyDeequ to create data profilling metrics in a PySpark DataFrame.

    Parameters
    --------
    spark : SparkSession
        A SparkSession that enables PyDeequ operations.
    df: pyspark.sql.DataFrame
        The DataFrame where the profilling will be aplied.
    """
    spark: SparkSession
    df: DataFrame
    analysis_runner: AnalysisRunner = field(init=False)

    def __attrs_post_init__(self):
        """Starts a PyDequu AnalysisRunner on self.df using self.spark."""
        self.analysis_runner = AnalysisRunner(self.spark).onData(self.df)

    def add_default_analysis(self, column: str):
        """Implements a basic profiling for a column.

        For all columns types, we are profiling:
        Completeness, Distinctness, Uniqueness.
        For numeric columns, we are profiling:
        Maximum, Minimum, Mean, StandardDeviation, Sum, and Quantiles.

        Parameters
        --------
        column: str
            The column name that will be profiled.
        """
        (self.analysis_runner
         .addAnalyzer(Completeness(column))
         .addAnalyzer(Distinctness(column))
         .addAnalyzer(Uniqueness([column]))
         .addAnalyzer(CountDistinct(column))
         .addAnalyzer(Maximum(column))
         .addAnalyzer(Minimum(column))
         .addAnalyzer(Mean(column))
         .addAnalyzer(StandardDeviation(column))
         .addAnalyzer(Sum(column))
         .addAnalyzer(ApproxQuantiles(column, [0.25, 0.50, 0.75], 0.0))
         )

    def all_columns(self):
        """Adds the default analysis for every column in self.df"""
        [self.add_default_analysis(column) for column in self.df.columns]

    def selected_columns(self, columns: list[str]):
        """Adds the default analysis for selected columns in self.df

        Parameters
        --------
        column: list[str]
            The list of columns name that will be profiled.
        """
        [self.add_default_analysis(column) for column in columns]

    def run_type(self, columns: list[str]):
        """Run analysis based on type.
        Can be all_columns or selected_columns"""
        if not columns:
            self.all_columns()
        else:
            self.selected_columns(columns)

    def run(self, columns: list[str] = []):
        """Returns a DataFrame with the result of the profiling.

        If there is no list of columns is empty,
        we run the default analysis in every column.
        If the list is not empty, we only run analysis on the list of columns.

        Parameters
        --------
        column: list[str]
            The list of columns name that will be profiled.
        """
        self.run_type(columns)
        analysis_results = self.analysis_runner.run()
        df_analysis_results = AnalyzerContext.successMetricsAsDataFrame(
            self.spark,
            analysis_results)
        return df_analysis_results
