from datetime import datetime

import numpy as np
import pandas as pd


class Calculator:
    """Data processing machinery.
    standard data schema: as_of_date, sid, data_value, kpi_value, period_from, period_to
    """

    def __init__(
        self,
        data=None,
        name="",
        on_spark=False,
        fixed_schema=(
            "as_of_date",
            "sid",
            "data_value",
            "kpi_value",
            "period_from",
            "period_to",
        ),
        multi_variate_names=tuple(), # expected tuple of predictors names, ordered same as in "data_value"
    ):
        """Constructor.
        :param data: pandas DataFrame, data to be fed directly to the class/object.
        :name: dataset name.
        :raise ValueError: if user provided data is not a panda DataFrame.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data is not a pandas DataFrame!")
        else:
            self.data = data.copy(deep=True)
            self.data.set_index(pd.Index(range(self.data.shape[0])))

        self._name = name
        self._on_spark = on_spark
        self._data_schema = set(fixed_schema)
        if not isinstance(multi_variate_names, tuple):
            raise ValueError("multi_variate_names must be tuple")
        else:
            self.multi_variate_names = list(multi_variate_names)
        today = pd.to_datetime(datetime.today()).tz_localize("UTC").tz_convert("EST")
        self._today = today.strftime("%Y-%m-%d")
        # do some initial quality check
        if not self.__quality_check():
            raise ValueError("Failed Data Quality Check")

    def __repr__(self):
        rep = "name: " + self._name
        return rep

    def __quality_check(self) -> bool:
        if self.data.shape[0] < 1:
            print("input DataFrame has 0 rows")
            return False
        elif not self.__check_schema():
            return False
        elif not self.__check_columns():
            return False
        return True

    def __check_schema(self) -> bool:
        """make sure the self.data has the assumed schema"""
        columns = self.data.columns
        if set(columns) == self._data_schema:
            return True
        else:
            print("not expected schema: ", self._data_schema)
            print("your input data schema: ", columns)
            return False

    def __check_columns(self) -> bool:
        try:
            self.data["as_of_date"] = pd.to_datetime(self.data["as_of_date"])
        except ValueError:
            raise ValueError("fail to convert column as_of_date to datetime")
        try:
            self.data["period_from"] = pd.to_datetime(self.data["period_from"])
        except ValueError:
            raise ValueError("fail to convert column period_from to datetime")
        try:
            self.data["period_to"] = pd.to_datetime(self.data["period_to"])
        except ValueError:
            raise ValueError("fail to convert column period_to to datetime")
        try:
            self.data["data_value"] = pd.to_numeric(
                self.data["data_value"]
            )  # no categorical features
        except ValueError:
            raise ValueError("fail to convert column data_value to numeric")
        try:
            self.data["kpi_value"] = pd.to_numeric(self.data["kpi_value"])
        except ValueError:
            raise ValueError("fail to convert column kpi_value to numeric")
        return True

    def calculate_coverage(self, vintage_date=None) -> pd.DataFrame:
        """assume no gap in coverage period, data imputation happend before so no 'dropna' here"""
        result = pd.DataFrame()
        all_sids = self.data["sid"].unique()
        for sid in all_sids:
            data_filtered = self.data.copy()
            data_filtered = data_filtered.loc[(data_filtered["sid"] == sid)]
            for as_of_date in data_filtered["as_of_date"].unique():
                coverage_from = data_filtered.loc[
                    (data_filtered["as_of_date"] <= as_of_date)
                ]["period_from"].min()
                coverage_to = data_filtered.loc[
                    (data_filtered["as_of_date"] <= as_of_date)
                ]["period_to"].max()
                cur_df = pd.DataFrame(
                    [[sid, as_of_date, coverage_from, coverage_to]],
                    columns=["sid", "as_of_date", "coverage_from", "coverage_to"],
                )
                result = pd.concat([result, cur_df])

        if not vintage_date:
            vintage_date = pd.to_datetime(vintage_date)
            result = result.loc[result["as_of_date"] <= vintage_date].sort_values(
                by="as_of_date", ascending=False
            )
            result = result.drop_duplicates(["sid"])
            
        result = result.set_index(pd.Index(range(result.shape[0])))
        return result

    def __calculate_stats_data_kpi_vintage_perTicker(
        self, sid, stats_method="pearson", vintage_date="2021-05-25"
    ) -> float:
        """covers both correlation and hit_rate"""
        try:
            vintage_date = pd.to_datetime(vintage_date)
        except ValueError:
            raise ValueError("fail to convert vintage_date to datetime type")
        data_filtered = self.data.copy()
        data_filtered = data_filtered.loc[data_filtered["sid"] == sid]
        data_filtered = data_filtered.loc[(data_filtered["as_of_date"] <= vintage_date)]

        idx = (
            data_filtered.groupby(["period_from"])["as_of_date"].transform(max)
            == data_filtered["as_of_date"]
        )
        data_filtered = data_filtered.loc[idx]

        cols = ["as_of_date", "sid", "data_value", "kpi_value"]
        data_filtered = data_filtered[cols].dropna()
        if data_filtered.shape[0] < 1:
            return np.nan

        if stats_method == "hit_rate":
            hits = sum(data_filtered["data_value"] * data_filtered["kpi_value"] > 0)
            hit_rate = hits / data_filtered.shape[0]
            return hit_rate
        elif stats_method == "pearson":
            return (
                data_filtered[["data_value", "kpi_value"]]
                .corr(method=stats_method)
                .loc["data_value", "kpi_value"]
            )
        elif callable(stats_method):
            try:
                return (stats_method(data_filtered["data_value"], data_filtered["kpi_value"]))
            except ValueError:
                return None


    def __calculate_stats_data_kpi_vintage(
        self, stats_method="pearson", vintage_date="2021-05-25"
    ) -> dict:
        result = dict()
        for sid in self.data["sid"].unique():
            result[sid] = self.__calculate_stats_data_kpi_vintage_perTicker(
                sid=sid, stats_method=stats_method, vintage_date=vintage_date
            )
        return result

    def __calculate_stats_data_kpi_noRevisions_perTicker(
        self, sid, stats_method="pearson"
    ) -> float:
        data_filtered = self.data.copy()
        cur_df = data_filtered.loc[(data_filtered["sid"] == sid)]
        cur_df = cur_df.dropna()
        if cur_df.shape[0] < 1:
            return np.nan

        cur_df["period"] = cur_df[["period_from", "period_to"]].apply(tuple, 1)
        idx = (
            cur_df.groupby(["period"])["as_of_date"].transform(min)
            == cur_df["as_of_date"]
        )
        this_df = cur_df.loc[idx, ["data_value", "kpi_value"]]
        if stats_method == "hit_rate":
            hits = sum(this_df["data_value"] * this_df["kpi_value"] > 0)
            hit_rate = hits / this_df.shape[0]
            return hit_rate
        elif stats_method == "pearson":
            return this_df.corr(method=stats_method).loc["data_value", "kpi_value"]
        elif callable(stats_method):
            try:
                return (stats_method(data_filtered["data_value"], data_filtered["kpi_value"]))
            except ValueError:
                return None

    def __calculate_stats_data_kpi_noRevisions(self, stats_method="pearson") -> dict:
        data_filtered = self.data.copy()
        result = dict()
        for sid in data_filtered["sid"].unique():
            result[sid] = self.__calculate_stats_data_kpi_noRevisions_perTicker(
                sid, stats_method=stats_method
            )
        return result

    def calculate_stats_data_kpi(
        self, stats_method="pearson", vintage_date=None
    ) -> dict:
        """
        accepted stats_method: pearson(correlation), hit_rate, callable function
        """
        result = dict()
        if not vintage_date:
            vintage_date = self._today

        if vintage_date == "all_versions":
            data_versions = self.data["as_of_date"].unique()
            for each_version in data_versions:
                result[each_version] = self.__calculate_stats_data_kpi_vintage(
                    stats_method=stats_method, vintage_date=each_version
                )
        elif vintage_date == "no_revisions":
            result[vintage_date] = self.__calculate_stats_data_kpi_noRevisions(
                stats_method=stats_method
            )
        else:
            result[vintage_date] = self.__calculate_stats_data_kpi_vintage(
                stats_method=stats_method, vintage_date=vintage_date
            )
        return result

    def __calculate_multi_predictors_perTicker(
        self, predictors, sid, stats_method="covariance", vintage_date=None
    ) -> pd.DataFrame:
        if vintage_date is None:
            vintage_date = self.vintage_date

        cur_df = predictors.loc[(predictors["sid"] == sid)]
        cur_df = cur_df.dropna()
        if cur_df.shape[0] < 1:
            return None
        cur_df = cur_df.loc[(cur_df["as_of_date"] <= vintage_date)]
        idx = (
            cur_df.groupby(["period_from"])["as_of_date"].transform(max)
            == cur_df["as_of_date"]
        )
        cur_df = cur_df.loc[idx]
        if stats_method == "covariance":
            return cur_df[self.multi_variate_names].cov()
        elif stats_method == "correlations":
            return cur_df[self.multi_variate_names].corr()
        elif callable(stats_method):
            try:
                return (stats_method(cur_df[self.multi_variate_names]))
            except ValueError:
                return None
        return None

    def __calculate_multi_predictors(
        self, predictors, stats_method="covariance", vintage_date=None
    ) -> dict:
        if vintage_date is None:
            vintage_date = self.vintage_date
        result = dict()
        for sid in predictors["sid"].unique():
            result[sid] = self.__calculate_multi_predictors_perTicker(
                predictors,
                sid=sid,
                stats_method=stats_method,
                vintage_date=vintage_date,
            )
        return result

    def __calculate_multi_predictors_noRevisions_perTicker(
        self, predictors, sid, stats_method="covariance", vintage_date=None
    ) -> pd.DataFrame:
        if vintage_date is None:
            vintage_date = self.vintage_date
        cur_df = predictors.loc[(predictors["sid"] == sid)]
        cur_df = cur_df.dropna()
        if cur_df.shape[0] < 1:
            return None
        cur_df = cur_df.loc[(cur_df["as_of_date"] <= vintage_date)]

        cur_df["period"] = cur_df[["period_from", "period_to"]].apply(tuple, 1)
        idx = (
            cur_df.groupby(["period"])["as_of_date"].transform(min)
            == cur_df["as_of_date"]
        )
        cur_df = cur_df.loc[idx]
        if stats_method == "covariance":
            return cur_df[self.multi_variate_names].cov()
        elif stats_method == "correlations":
            return cur_df[self.multi_variate_names].corr()
        return None

    def __calculate_multi_predictors_noRevisions(
        self, predictors, stats_method="covariance", vintage_date=None
    ) -> dict:
        if vintage_date is None:
            vintage_date = self.vintage_date
        result = dict()
        for sid in predictors["sid"].unique():
            result[sid] = self.__calculate_multi_predictors_noRevisions_perTicker(
                predictors,
                sid=sid,
                stats_method=stats_method,
                vintage_date=vintage_date,
            )
        return result

    def multi_predictors_interaction_stats(
        self, stats_method="covariance", vintage_date=None
    ) -> dict:
        s = self.data["data_value"]
        predictors = pd.DataFrame.from_dict(dict(zip(s.index, s.values))).T
        predictors.columns = self.multi_variate_names
        (
            predictors["sid"],
            predictors["as_of_date"],
            predictors["period_from"],
            predictors["period_to"],
        ) = (
            self.data["sid"],
            self.data["as_of_date"],
            self.data["period_from"],
            self.data["period_to"],
        )
        result = dict()
        if vintage_date is None:
            vintage_date = self._today
        if vintage_date == "all_versions":
            data_versions = predictors["as_of_date"].unique()
            for each_version in data_versions:
                result[each_version] = self.__calculate_multi_predictors(
                    predictors, stats_method=stats_method, vintage_date=each_version
                )
        elif vintage_date == "no_revisions":
            result[vintage_date] = self.__calculate_multi_predictors_noRevisions(
                stats_method=stats_method
            )
        else:
            result[vintage_date] = self.__calculate_multi_predictors(
                predictors, stats_method=stats_method, vintage_date=vintage_date
            )
        return result