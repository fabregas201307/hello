import json
import datetime
import pandas as pd
import numpy as np


class Aligner:
    """
    Data aligning machinery.
    """

    def __init__(
        self,
        data=pd.DataFrame(): pd.DataFrame,
        calendar=pd.DataFrame(): pd.DataFrame,
        data_schema=(
            "valid_from",
            "valid_to",
            "period_from",
            "period_to",
            "value"
        ),
        calendar_schema=(
            "period_from",
            "period_to",
            "as_of"
        ),
        transformation="raw": str,
        backtest_Vintage="2021-07-14": str,
        aggregation_type="sum": str,
        periodicity_data="quarterly": str,
        periodicity_kpi="quarterly": str
    ):
        self.data = data
        self.calendar = calendar
        self._data_schema = set(data_schema)
        self._calendar_schema = set(calendar_schema)
        self.backtest_Vintage = backtest_Vintage
        self.transformation = transformation

        if "valid_from" not in self.data.columns:
            self.data["valid_from"] = self.data["period_to"]

        if "as_of" not in self.calendar.columns:
            if backtest_Vintage == "PIT":
                self.calendar["as_of"] = self.data["period_to"]  ### may need t+10
            elif len(backtest_Vintage) < 1:
                self.calendar["as_of"] = pd.to_datetime(datetime.today()).tz_localize("UTC").tz_convert("EST") # convert to EST
            else:
                self.calendar["as_of"] = pd.to_datetime(backtest_Vintage)

        if not self.__quality_check():
            raise ValueError("Failed Data Quality Check.")

    def __quality_check(self) -> bool:
        if self.data.shape[0] < 1 or self.calendar.shape[0] < 1:
            print("input timeseries or calendar has 0 rows")
            return False
        return self.__check_schema() and self.__check_columns()

    def __check_columns(self) -> bool:
        try:
            self.data["valid_from"] = pd.to_datetime(self.data["valid_from"])
        except ValueError:
            raise ValueError("fail to convert data column valid_from to datetime")
        try:
            self.data["valid_to"] = pd.to_datetime(self.data["valid_to"])
        except ValueError:
            raise ValueError("fail to convert data column valid_to to datetime")
        try:
            self.data["period_from"] = pd.to_datetime(self.data["period_from"])
        except ValueError:
            raise ValueError("fail to convert data column period_from to datetime")
        try:
            self.data["period_to"] = pd.to_datetime(self.data["period_to"])
        except ValueError:
            raise ValueError("fail to convert data column period_to to datetime")
        try:
            self.data["value"] = pd.to_numeric(self.data["value"])  ## no categorical features
        except ValueError:
            raise ValueError("fail to convert data column value to numeric")

        try:
            calendar_period_from = pd.to_datetime(self.calendar["period_from"])
            self.calendar["period_from"] = calendar_period_from.sort_values()
        except ValueError:
            raise ValueError("fail to convert calendar column period_from to datetime")
        try:
            self.calendar["period_to"] = pd.to_datetime(self.calendar["period_to"])
        except ValueError:
            raise ValueError("fail to convert calendar column period_to to datetime")
        try:
            self.calendar["as_of"] = pd.to_datetime(self.calendar["as_of"])
        except ValueError:
            raise ValueError("fail to convert calendar column as_of to datetime")

        return True

    def __check_schema(self) -> bool:
        """make sure the self.data and self.calendar has the assumed schema"""
        if set(self.data.columns) != self._data_schema:
            print("not expected data schema: ", self._data_schema)
            print("your input data schema: ", set(self.data.columns))
            return False
        elif set(self.calendar.columns) != self._calendar_schema:
            print("not expected calendar schema: ", self._calendar_schema)
            print("your input calendar schema: ", set(self.calendar.columns))
            return False
        return True

    def aggregate_to_calendar(self, data=None, calendar=None) -> pd.DataFrame:
        result = pd.DataFrame(columns=["period_from", "period_to", "aggregated_value"])
        if not isinstance(data, pd.DataFrame):
            print ("use object's data pandas dataframe")
            data=self.data.copy()
        if not isinstance(calendar, pd.DataFrame):
            print ("use object's calendar pandas dataframe")
            calendar=self.calendar.copy()

        for idx in calendar.index:
            as_of, period_from, period_to = calendar.loc[idx, "as_of"], calendar.loc[idx, "period_from"], calendar.loc[idx, "period_to"]
            data_versioned = data.copy()
            data_versioned = data_versioned.loc[data_versioned["valid_from"] <= as_of].sort_values(by="valid_from", ascending=False)
            data_versioned = data_versioned.drop_duplicates(["period_from", "period_to"])
            # only one version per period from here
            data_in_period = data_versioned.loc[(data_versioned["period_from"] >= period_from) & (data_versioned["period_to"] <= period_to)]
            values_in_period = data_in_period["value"]
            value_aggregated = values_in_period.sum() if (aggregation_type == "sum") else values_in_period.mean()  # to do: include more aggregation options
            cur_df = pd.DataFrame([[period_from, period_to, value_aggregated]], columns=["period_from", "period_to", "aggregated_value"])
            result = pd.concat([result, cur_df])
        result.set_index(pd.Index(range(result.shape[0])))
        return result

    def generate_feature(
        self,
        frequency_lag_days= {'YoY':364, 'semi-annually':182, 'QoQ':88, 'MoM':26, 'WoW':6}: dict,
        method = "percentage"
    ):
        result = pd.DataFrame(columns=["period_from", "period_to", "feature"])

        backtest_Vintage = self.backtest_Vintage
        transformation = self.transformation

        for idx in self.calendar.index:
            as_of, period_from, period_to = self.calendar.loc[idx, "as_of"], self.calendar.loc[idx, "period_from"], self.calendar.loc[idx, "period_to"]
            calendar_slice = self.calendar.copy()
            if backtest_Vintage == "PIT":
                calendar_slice["as_of"] = as_of
            calendar_slice = calendar_slice.loc[:idx]
            cur_feature = self.__get_numeric_feature(transformation=self.transformation, method=method, frequency_lag_days=frequency_lag_days, data=self.data, calendar_slice=calendar_slice)
            cur_df = pd.DataFrame([period_from, period_to, cur_feature], columns=["period_from", "period_to", "feature"])
            result = pd.concat([result, cur_df])
        return result

    def __get_numeric_feature(
        self,
        transformation,
        method,
        frequency_lag_days,
        data,
        calendar_slice
    ) -> float:
        df_aggregated = self.aggregate_to_calendar(data=data, calendar=calendar_slice)

        # if transformation == "raw":

        idx = df_aggregated.index[-1]
        cur_period_from, cur_value = df_aggregated.loc[idx, "period_from"], df_aggregated.loc[idx, "aggregated_value"]
        cur_period_from_lag = cur_period_from  - pd.to_timedelta(frequency_lag_days[transformation], "d")
        try:
            prev_value = df_aggregated.loc[df_aggregated["period_from"]<=cur_period_from_lag].tail(1)["aggregated_value"]
        except ValueError:
            prev_value = np.nan

        timeseries_slice = df_aggregated.loc[(df_aggregated["period_from"]>=cur_period_from_lag) & (df_aggregated["period_from"]<=cur_period_from)]["aggregated_value"]

        if method == "percentage":
            cur_feature = (cur_value - prev_value) / abs(prev_value)
        elif method == "diff":
            cur_feature = cur_value - prev_value
        elif method == "log10_percentage":
            cur_feature = np.log10((cur_value - prev_value) / abs(prev_value))
        elif method == "volatility":
            cur_feature = timeseries_slice.std(ddof=0)
        elif method == "slope":
            cur_feature = np.polyfit(timeseries_slice.index, timeseries_slice, 1)[0]
        elif method == "convexity":
            cur_feature = np.polyfit(timeseries_slice.index, timeseries_slice, 2)[0]

        return cur_feature




