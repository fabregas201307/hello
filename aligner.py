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
        self._today = (
            pd.to_datetime(datetime.today()).tz_localize("UTC").tz_convert("EST")
        )
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

    def calculate_single_coverage(self, target_col="data_value") -> dict:
        result = dict()
        tuples = list(zip(self.data["period_from"], self.data["period_to"]))
        for period in tuples:
            data_filtered = self.data.copy()
            data_filtered = data_filtered.loc[
                (data_filtered["period_from"] == period[0])
                & (data_filtered["period_to"] == period[1])
            ]
            cols = ["as_of_date", "sid", target_col]
            data_filtered = data_filtered[cols].dropna()
            try:
                result[period] = (
                    data_filtered.groupby("as_of_date")["sid"].apply(set).to_dict()
                )
            except ValueError:
                result[period] = dict()
        return result

    def calculate_correlations_data_kpi(self, correlation_method="pearson") -> dict:
        result = dict()
        timestamps = self.data["as_of_date"].unique()
        for as_of_date in timestamps:
            data_filtered = self.data.copy()
            data_filtered = data_filtered.loc[
                (data_filtered["as_of_date"] == as_of_date)
            ]

            data_filtered = data_filtered.loc[
                (data_filtered["period_to"] <= as_of_date)
            ]
            # above line should be redundant

            cols = ["as_of_date", "sid", "data_value", "kpi_value"]
            data_filtered = data_filtered[cols].dropna()
            result[as_of_date] = (
                data_filtered[["data_value", "kpi_value"]]
                .corr(method=correlation_method)
                .loc["data_value", "kpi_value"]
            )
        return result

    # def calculate_feature_per_security(
    #     self,
    #     cur_security: str,
    #     target_col: str = "data_value",
    #     frequency: str = "quarterly",
    #     frequency_lag_days: dict = {
    #         "annually": 360,
    #         "semi-annually": 180,
    #         "quarterly": 88,
    #         "monthly": 25,
    #         "weekly": 6,
    #     },
    #     method="percentage",
    # ) -> pd.Series:
    #     """always assume the period is 1-1 map with one fixed frequency"""

    #     if method not in ["percentage", "diff", "log10_percentage"]:
    #         raise ValueError(
    #             "transformation not covered, must be one of 'percentage', 'diff', 'log10_percentage'"
    #         )
    #     elif frequency not in [
    #         "annually",
    #         "semi-annually'",
    #         "quarterly",
    #         "monthly",
    #         "weekly",
    #     ]:
    #         raise ValueError(
    #             "frequency not covered, must be one of 'annually', 'semi-annually', 'quarterly', 'monthly', 'weekly'"
    #         )

    #     data_filtered = self.data.copy()
    #     data_filtered = data_filtered.loc[data_filtered["sid"] == cur_security]
    #     data_versions = data_filtered["as_of_date"].unique()
    #     # data_filtered.index = range(data_filtered.shape[0])
    #     result = pd.Series()
    #     for cur_version in data_versions:
    #         df = data_filtered.loc[data_filtered["as_of_date"] == cur_version]
    #         cols = [target_col, "period_from", "period_to"]
    #         df = df[cols].drop_duplicates()
    #         df = df.sort_values(
    #             "period_from", ascending=True
    #         )  # assume no time period overlap

    #         cur_version_list = list()

    #         for idx in df.index:
    #             cur_period_from, cur_value = (
    #                 df.loc[idx, "period_from"],
    #                 df.loc[idx, target_col],
    #             )
    #             cur_period_from_lag = cur_period_from - pd.to_timedelta(
    #                 frequency_lag_days[frequency], "d"
    #             )
    #             try:
    #                 prev_value = df[df["period_from"] <= cur_period_from_lag].tail(1)[
    #                     "data_value"
    #                 ]
    #             except ValueError:
    #                 prev_value = np.nan

    #             if method == "percentage":
    #                 cur_feature = (cur_value - prev_value) / abs(prev_value)
    #             elif method == "diff":
    #                 cur_feature = cur_value - prev_value
    #             elif method == "log10_percentage":
    #                 cur_feature = np.log10((cur_value - prev_value) / abs(prev_value))

    #             cur_version_list.append(cur_feature)

    #         cur_version_series = pd.Series(cur_version_list, index=df.index)
    #         result = pd.concat(result, cur_version_series)
    #     return result

    # def calculate_feature(
    #     self,
    #     target_col: str = "data_value",
    #     frequency: str = "quarterly",
    #     frequency_lag_days: dict = {
    #         "annually": 360,
    #         "semi-annually": 180,
    #         "quarterly": 88,
    #         "monthly": 25,
    #     },
    #     method="percentage",
    # ) -> pd.Series:
    #     """always assume the period is 1-1 map with one fixed frequency"""
    #     if method not in ["percentage", "diff", "log10_percentage"]:
    #         raise ValueError(
    #             "transformation not covered, must be one of 'percentage', 'diff', 'log10_percentage'"
    #         )
    #     elif frequency not in [
    #         "annually",
    #         "semi-annually'",
    #         "quarterly",
    #         "monthly",
    #         "weekly",
    #     ]:
    #         raise ValueError(
    #             "frequency not covered, must be one of 'annually', 'semi-annually', 'quarterly', 'monthly', 'weekly'"
    #         )
    #     all_sid = self.data["sid"].unique()
    #     result = pd.Series()
    #     for sid in all_sid:
    #         result = pd.concat(
    #             [
    #                 result,
    #                 self.calculate_feature_per_security(
    #                     cur_security=sid,
    #                     target_col=target_col,
    #                     frequency=frequency,
    #                     frequency_lag_days=frequency_lag_days,
    #                     method=method,
    #                 ),
    #             ]
    #         )
    #     return result

    def calculate_pairwise_coverage(self) -> dict:
        result = dict()
        tuples = list(zip(self.data["period_from"], self.data["period_to"]))
        for period in tuples:
            data_filtered = self.data.copy()
            data_filtered = data_filtered.loc[
                (data_filtered["period_from"] == period[0])
                & (data_filtered["period_to"] == period[1])
            ]
            cols = ["as_of_date", "sid", "data_value", "kpi_value"]
            data_filtered = data_filtered[cols].dropna()
            try:
                result[period] = (
                    data_filtered.groupby("as_of_date")["sid"].apply(set).to_dict()
                )
            except ValueError:
                result[period] = dict()
        return result

    def calculate_coverage(self) -> pd.DataFrame:
        """assume no gap in coverage period, data imputation happend before so no 'dropna' here"""
        result = pd.DataFrame()
        all_sids = self.data["sid"].unique()
        for sid in all_sids:
            data_filtered = self.data.copy()
            data_filtered = data_filtered.loc[(data_filtered["sid"] == sid)]
            for as_of_date in data_filtered["as_of_date"].unique():
                coverage_from = data_filtered.loc[
                    (data_filtered["as_of_date"] == as_of_date)
                ]["period_from"].min()
                coverage_to = data_filtered.loc[
                    (data_filtered["as_of_date"] == as_of_date)
                ]["period_to"].max()
                cur_df = pd.DataFrame(
                    [[sid, as_of_date, coverage_from, coverage_to]],
                    columns=["sid", "as_of_date", "coverage_from", "coverage_to"],
                )
                result = pd.concat([result, cur_df])
        result = result.set_index(pd.Index(range(result.shape[0])))
        return result

    def calculate_correlations_data_kpi_previous_Version(
        self, correlation_method="pearson"
    ) -> dict:
        result = dict()
        timestamps = self.data["as_of_date"].unique()
        for as_of_date in timestamps:
            data_filtered = self.data.copy()
            data_filtered = data_filtered.loc[
                (data_filtered["as_of_date"] == as_of_date)
            ]

            data_filtered = data_filtered.loc[
                (data_filtered["period_to"] <= as_of_date)
            ]
            # above line should be redundant

            cols = ["as_of_date", "sid", "data_value", "kpi_value"]
            data_filtered = data_filtered[cols].dropna()
            result[as_of_date] = (
                data_filtered[["data_value", "kpi_value"]]
                .corr(method=correlation_method)
                .loc["data_value", "kpi_value"]
            )
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
        else:
            return (
                data_filtered[["data_value", "kpi_value"]]
                .corr(method=stats_method)
                .loc["data_value", "kpi_value"]
            )

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
        else:
            return this_df.corr(method=stats_method).loc["data_value", "kpi_value"]

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
