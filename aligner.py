import pandas as pd

class Aggregator:
    """aggregate to universe summary statistics"""

    def __init__(
        self,
        stats: pd.DataFrame,
        stats_schema=("backtest_date", "backtest_type", "backtest_criteria", "data_security", "data_vendor",
            "data_definition", "kpi_security", "kpi_name", "score_metric", "period_from", "period_to", "score_value"),
    ):

        if not isinstance(stats, pd.DataFrame):
            raise ValueError("stats is not a pandas DataFrame!")

        self.stats = stats
        self._stats_schema = set(stats_schema)

        if not self.__quality_check():
            raise ValueError("Failed Data Quality Check.")

    def __check_columns(self) -> bool:
        try:
            self.stats["backtest_date"] = pd.to_datetime(self.stats["backtest_date"])
        except ValueError:
            raise ValueError("fail to convert stats column backtest_date to datetime")
        try:
            self.stats["period_from"] = pd.to_datetime(self.stats["period_from"])
        except ValueError:
            raise ValueError("fail to convert stats column period_from to datetime")
        try:
            self.stats["period_to"] = pd.to_datetime(self.stats["period_to"])
        except ValueError:
            raise ValueError("fail to convert stats column period_to to datetime")
        try:
            self.stats["score_value"] = pd.to_numeric(
                self.stats["score_value"]
            )  ## no categorical features
        except ValueError:
            raise ValueError("fail to convert stats column score_value to numeric")
        return True

    def __check_schema(self) -> bool:
        """make sure the self.data and self.calendar has the assumed schema"""
        if set(self.stats.columns) != self._stats_schema:
            print("not expected data schema: ", self._stats_schema)
            print("your input data schema: ", set(self.stats.columns))
            return False
        return True

    def __quality_check(self) -> bool:
        if self.stats.shape[0] < 1:
            print("input statistics has 0 rows")
            return False
        return self.__check_schema() and self.__check_columns()

    def summary_stats(self, score_metric_filter=None, group_by=["data_security", "backtest_type"], aggregation_type="mean") -> pd.DataFrame:
        sum_miss = sum([(j not in self._stats_schema) for j in group_by])
        supported_aggregations = ["mean", "median", "max", "min"]
        if sum_miss > 0:
            raise ValueError("summary_stats group_by arguments outside accepted schema.")
        elif aggregation_type not in supported_aggregations:
            raise ValueError("summary_stats aggregation_type not supported, here are supported list: ", supported_aggregations)

        my_stats = self.stats.copy()
        if not score_metric_filter:
            my_stats = my_stats.loc[my_stats["score_metric"] == score_metric_filter]


        if aggregation_type=="mean":
            result = self.stats.groupby(group_by).mean("score_value")
        elif aggregation_type=="median":
            result = self.stats.groupby(group_by).median("score_value")
        elif aggregation_type=="max":
            result = self.stats.groupby(group_by).max("score_value")
        elif aggregation_type=="min":
            result = self.stats.groupby(group_by).min("score_value")

        return result.reset_index()
