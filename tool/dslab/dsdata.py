
import math
import operator
import datetime
import numpy as np
import pandas as pd
from functools import wraps
from functools import partial
from sklearn import preprocessing
from pandas.tseries.offsets import BDay
from sklearn.covariance import MinCovDet
from scipy.stats import percentileofscore
from pandas.tseries.holiday import USFederalHolidayCalendar

from dslab.mlutils import merge_dicts
from dslab.dtypes.dtype_codes import DTYPES
from dslab.config import init_logger, log_fun_info
from dslab.mlutils import list_of_dict_to_dict_of_list

logger = init_logger(__name__, 'warn')


def _generative(func):
    """Create a decorator to enable function chains.
    :param func: function to be decorated.
    :return: the decorated function.
    """
    @wraps(func)
    def decorator(self, *args, **kw):
        """
        :param self:
        :param args:
        :param kw:
        :return:
        """
        new_self = self.__class__.__new__(self.__class__)
        new_self.__dict__ = self.__dict__.copy()
        func(new_self, *args, **kw)
        return new_self
    return decorator


class MLData():
    """Data processing machinery.
    **Class Attributes**:
    :py:attr:`self.data`: DataFrame, store the data (start with empty DataFrame).
    :py:attr:`self.data_snapshot`: DataFrame, store certain snapshot of data.
    :py:attr:`self.dummies`: DataFrame for dummy data.
    """

    def __init__(self, data=None, regret=False):
        """Constructor.
        :param data: pandas DataFrame, data to be fed directly to the class/object.
        :param regret: boolean, whether to store previous copy of data before changes.
        :raise ValueError: if user provided data is not a panda DataFrame.
        """
        if data is not None:
            if not isinstance(data, pd.DataFrame):
                raise ValueError('Data is not a pandas DataFrame!')
            self.data = data.copy(deep=True)
        else:
            self.data = pd.DataFrame()
        self.data_snapshot = pd.DataFrame()
        self.dummies = pd.DataFrame()
        self.prev_data = pd.DataFrame()
        self.regret = regret
        self._filter = None
        self._sort = None

    def read_data(self, filepath=None, filepaths=None, low_memory=False, encoding='ISO-8859-1',
                  thousands=',', infer_cat=True, parse_dates=False):
        """Read data from csv file.
        :param filepath: str, the file path to read the csv file.
        :param filepaths: list, the list of filepaths to read csv files.
        :param low_memory: bool, low memory usage.
        :param encoding: str, data encoding.
        :param thousands: str, delimiter to for thousands.
        :param infer_cat: bool, automatically infer categorical variables.
        :param parse_dates: bool or list of date column names.
        :raise ValueError:
        """
        if filepath is None and filepaths is None:
            raise ValueError("File path is not provided!")
        if filepaths is not None:
            logger.debug('Files to read: {}'.format(filepaths))
            for filepath in filepaths:
                if filepath.find('.csv') != -1:
                    temp = pd.read_csv(filepath, low_memory=low_memory, encoding=encoding,
                                       thousands=thousands, parse_dates=parse_dates, error_bad_lines=False)
                elif filepath.find('.xlsx') != -1 or filepath.find('.xls') != -1:
                    temp = pd.read_excel(filepath, encoding=encoding,
                                         thousands=thousands, parse_dates=parse_dates)
                else:
                    raise ValueError("Only .csv or .xlsx files are accepted!")
                self.data = pd.concat([self.data, temp])
            self.data.reset_index(drop=True, inplace=True)
        else:
            logger.debug('File to read: {}'.format(filepath))
            if filepath.find('.csv') != -1:
                self.data = pd.read_csv(filepath, low_memory=low_memory, encoding=encoding,
                                        thousands=thousands, parse_dates=parse_dates, error_bad_lines=False)
            elif filepath.find('.xlsx') != -1 or filepath.find('.xls') != -1:
                self.data = pd.read_excel(filepath, thousands=thousands, parse_dates=parse_dates)
            else:
                raise ValueError("Only .csv or .xlsx files are accepted!")

        if infer_cat:
            self.infer_categorical()

    # def no_go_back(self):
    #     """Destroy the copies in previous step.
    #     """
    #     self.regret = False
    #     self.prev_data = pd.DataFrame()

    # def go_back(self):
    #     """Restore to previous step"""
    #     if self.regret and len(self.prev_data) > 0:
    #         self.data = self.prev_data
    #         self.prev_data = pd.DataFrame()

    def save_current(self):
        """Save current snapshot of data as self.data_snapshot.
        Save a (deep) copy of the data.
        """
        self.data_snapshot = self.data.copy(deep=True)

    def drop_cols(self, columns, inplace=True):
        """Drop a set of columns.
        :param columns: list, columns to drop.
        :param inplace: boolean, alter the data in place or not.
        :return: if inplace == False, return the data frame; else None.
        """
        if inplace and self.regret:
            self.prev_data = self.data.copy(deep=True)
        columns = np.intersect1d(columns, self.data.columns)
        return self.data.drop(columns, axis=1, inplace=inplace)

    def get_dtypes(self):
        """Get data types and (when possible) categorical values.
        :return: data types as dictionary.
        """
        res = {}
        for column in self.data.columns:
            for dtype in DTYPES:
                if dtype in str(self.data[column].dtype):
                    res[column] = dtype
                    break
            if column not in res:
                logger.error('{} seems like a new data type! Please contact the developer to update the library.'
                             .format(self.data[column].dtype))
                res[column] = 'object'
        return res

    def get_summary(self, sortby=None):
        """Get summary statistics for each columns
        :param sortby: str, column name to sort by.
        :return: summary stats.
        """
        summary = self.data.describe(include='all').T
        summary['nulls'] = self.data.isnull().sum()
        dtypes = pd.DataFrame.from_dict(self.get_dtypes(), orient='index')
        dtypes.columns = ['dtype']
        summary = dtypes.join(summary)
        summary = summary.reset_index().rename(columns={'index': 'column name'})
        summary = summary.rename(columns={"top": "mode", "freq": "mode_freq", "50%": "50% (median)"})
        columns = ["column name", "dtype", "unique", "count", "nulls", "mode", "mode_freq",
                   "mean", "std", "min", "25%", "50% (median)", "75%", "max"]
        summary.reset_index(drop=True, inplace=True)
        summary = summary.reindex(columns=columns)
        summary = summary.round(4)
        if sortby is not None:
            summary.sort_values(sortby, inplace=True)
        return summary

    def get_categories(self):
        """Get categories for all categorical data.
        :return: categories as list for each categorical column (as dictionary).
        """
        return dict((column, list(self.data[column].cat.categories))
                    for column in self.data.columns if str(self.data[column].dtype) == 'category')

    def get_header(self, dtype=None, sort=False):
        """get column header for certain types.
        :param dtype: str or list, data type or list of data types.
        :param sort: bool, sort results.
        :return: list of column header.
        """
        if dtype is None:
            res = list(self.data.columns)
        else:
            if isinstance(dtype, str):
                dtypes = [dtype]
            else:
                dtypes = list(dtype)

            res = [column for column in self.data.columns for ty in dtypes
                   if ty in str(self.data[column].dtype)]
        if sort:
            res.sort()
        return res

    def infer_categorical(self, columns=None, exclude_cols=None, threshold=500):
        """Infer categorical columns in the data.
        :param columns: list, columns to process.
        :param exclude_cols: list, columns to exclude.
        :param threshold: threshold on the number of unique values in a column.
        """
        columns = self.__set_columns_input(columns, exclude_cols)

        if self.regret:
            self.prev_data = self.data.copy(deep=True)

        dtypes = self.get_dtypes()
        for column in columns:
            if dtypes[column] == 'category':
                if len(self.data[column].unique()) > threshold:
                    logger.debug("Convert {} back to 'object' b/c it has more than {} unique values.".
                                 format(column, threshold))
                    self.data[column] = self.data[column].astype('object')
                else:
                    self.data[column] = self.data[column].cat.remove_unused_categories()
            elif dtypes[column] == 'object':
                if len(self.data[column].unique()) > threshold:
                    logger.debug('"{}" has more than {} unique values. I won\'t parse it into categorical '
                                 'column.'.format(column, threshold))
                    continue
                try:
                    self.data[column] = self.data[column].astype('category')
                except Exception as e:
                    logger.error('Can\'t parse {} into categorical column. \n Error message: {}'.
                                 format(column, e))

    def drop_cat_cols(self, columns=None, exclude_cols=None, threshold=500, inplace=True):
        """Drop categorical columns by certain threshold.
        :param columns: list, the set of column names to check. (note that you can't set both
                        'columns' and 'exclude_cols' at the same time.)
        :param exclude_cols: list, the set of column names excluded from check. (note that you
                             can't set both 'columns' and 'exclude_cols' at the same time.)
        :param threshold: int, the maximum number of categories allowed for each categorical column.
        :param inplace: boolean, alter the data in place or not.
        :return: if inplace == False, return the data frame; else None.
        :raise ValueError:
        """
        columns = self.__set_columns_input(columns, exclude_cols)

        cols_to_delete = []
        for column in columns:
            column_unqiue_counts = len(self.data[column].unique())
            if column_unqiue_counts <= 1 or \
                    (str(self.data[column].dtype) in ['object', 'category'] and column_unqiue_counts > threshold):
                logger.debug('{} deleted due to size {}'.format(column, column_unqiue_counts))
                cols_to_delete += [column]
        if len(cols_to_delete) > 0:
            return cols_to_delete, self.drop_cols(cols_to_delete, inplace=inplace)

    def __set_columns_input(self, columns, exclude_cols):
        """Set columns input for fillna(), scale_numeric() and create_dummy().
        :param columns: list, set of cadidate columns.
        :param exclude_cols: list, columns to be excluded.
        :raise ValueError:
        :return: return columns
        """
        if columns is not None and exclude_cols is not None:
            raise ValueError("Can't set both 'columns' and 'exclude_cols' at the same time!")

        if exclude_cols is not None:
            columns = self.data.columns.difference(exclude_cols)
        elif columns is not None:
            columns = np.intersect1d(columns, self.data.columns)
        else:
            columns = self.data.columns
        return columns

    def parse_dates(self, columns, inplace=True):
        """parse string into dates.
        :param columns: list of columns to parse.
        :param inplace: boolean, alter the data in place or not.
        :return: if not inplace, return the parsed columns.
        """
        columns = np.intersect1d(columns, self.data.columns)

        if inplace and self.regret:
            self.prev_data = self.data.copy(deep=True)

        res = self.data[columns].apply(lambda x: pd.to_datetime(x, infer_datetime_format=True))

        if inplace:
            self.data[columns] = res
        else:
            return res

    def ct_freq(self, column, group_less_freq=False, limit=None, normalize=False, bins=None, sort=True, dropna=True):
        """Count frequence for each value in certain columns.
        :param column: str, column name.
        :param group_less_freq: bool, group less frequent values as 'others'.
        :param limit: int, limit for group_less_freq.
        :param normalize: bool, relative vs absolute.
        :param bins: int, group values into half-open bins.
        :param sort: bool, sort by values.
        :param dropna: bool, drop nulls.
        :return: frequence count.
        """
        vc = self.data[column].value_counts(normalize, bins=bins, sort=sort, dropna=dropna)

        if group_less_freq:
            if not sort:
                vc.sort_values(ascending=False, inplace=True)
            if limit is None:
                limit = 20
            others_sum = vc.iloc[limit:].sum()
            vc = vc.iloc[:limit]
            if others_sum > 0:
                vc = pd.Series(list(vc) + [others_sum], list(vc.index) + ['others'])
        elif limit is not None:
            vc = vc.iloc[:limit]
        return vc


    def head(self, n=5):
        """ Return first n rows.
        :param n: number of rows to show.
        :return: first n rows
        """
        return self.data.head(n)

    def tail(self, n=5):
        """Return last n rows.
        :param n: number of rows to show.
        :return: last n rows
        """
        return self.data.tail(n)

    def shape(self):
        """Return row number and column number.
        :return: data shape
        """
        return self.data.shape

    def parse_str(self, columns, method, strip=True, inplace=False, *args, **kwargs):
        """Parse string.
        :param columns: list of string columns.
        :param method: string, method to apply. Options include every string handling functions listed
            here http://pandas.pydata.org/pandas-docs/stable/api.html#string-handling.
        :param strip: strip white spaces or not.
        :param inplace: boolean, alter the data in place or not.
        :param method_params: additional parameters for the method.
        :return: parsed results.
        :raise ValueError:
        """
        log_fun_info(logger)
        if inplace and self.regret:
            self.prev_data = self.data.copy(deep=True)

        if not isinstance(columns, str) and len(columns) == 1:
            columns = columns[0]

        if isinstance(columns, str):
            if columns in self.data.columns:
                if strip:
                    res = getattr(self.data[columns].astype(str).str.strip().str, method)(*args, **kwargs)
                else:
                    res = getattr(self.data[columns].astype(str).str, method)(*args, **kwargs)
            else:
                raise ValueError('"{}" is not contained in the data.'.format(columns))
        else:
            columns = np.intersect1d(columns, self.data.columns)
            if strip:
                res = self.data[columns].apply(lambda x: getattr(x.astype(str).str.strip().str, method)(*args, **kwargs))
            else:
                res = self.data[columns].apply(lambda x: getattr(x.astype(str).str, method)(*args, **kwargs))

        if inplace:
            self.data[columns] = res
        else:
            return res

    def create_dummy_data(self, columns=None, exclude_cols=None, **extra_kwargs):
        """Create dummy data.
        :param columns: list, the set of column names chosen for creating dummies. (note that you
                        can't set both 'columns' and 'exclude_cols' at the same time.)
        :param exclude_cols: list, the set of column names excluded for creating dummies. (note that
                             you can't set both 'columns' and 'exclude_cols' at the same time.)
        """
        columns = self.__set_columns_input(columns, exclude_cols)
        self.dummy_cols = list(columns)
        self.dummies = pd.get_dummies(self.data[columns], prefix_sep="___", **extra_kwargs)
        logger.debug('converted the following to dumy columns: {}'.format(list(columns)))

    def calculate_YoY(self, target):
        '''expect self.data has columns date, sid, and the feature column to work on'''
        df = self.data.copy()
        df['date'] = pd.to_datetime(df['date'])
        delta_1yr_left, delta_1yr_right = pd.to_timedelta(395, 'd'), pd.to_timedelta(360, 'd')
        df["date_1yr_lag_left"], df["date_1yr_lag_right"] = (df['date'] - delta_1yr_left), (df['date'] - delta_1yr_right)

        def each_row(x, target=target):
            left_date, right_date = x['date_1yr_lag_left'], x['date_1yr_lag_right']
            filter_df = df[(df['date'] > left_date) & (df['date'] < right_date)]
            if filter_df.shape[0]<1:
                return(np.nan)            
            kpi_current, kpi_prev = x[target], filter_df.tail(1).squeeze()[target]
            try:
                return(kpi_current/kpi_prev - 1)
            except:
                return(np.nan)
        result = df.apply(lambda x : each_row(x, target=target), axis=1)
        return result

    def calculate_QoQ(self, target):
        '''expect self.data has columns date, sid, and the feature column to work on'''
        df = self.data.copy()
        df['date'] = pd.to_datetime(df['date'])
        delta_1Q_left, delta_1Q_right = pd.to_timedelta(120, 'd'), pd.to_timedelta(88, 'd')
        df["date_1Q_lag_left"], df["date_1Q_lag_right"] = (df['date'] - delta_1Q_left), (df['date'] - delta_1Q_right)

        def each_row(x, target=target):
            left_date, right_date = x['date_1Q_lag_left'], x['date_1Q_lag_right']
            filter_df = df[(df['date'] > left_date) & (df['date'] < right_date)]
            if filter_df.shape[0]<1:
                return(np.nan)            
            kpi_current, kpi_prev = x[target], filter_df.tail(1).squeeze()[target]
            try:
                return(kpi_current/kpi_prev - 1)
            except:
                return(np.nan)
        result = df.apply(lambda x : each_row(x, target=target), axis=1)
        return result

    def calculate_QQYY(self, target):
        '''expect self.data has columns date, sid, and the feature column to work on'''
        # df = self.data.copy()
        self.data[target+'$QoQ'] = self.calculate_QoQ(target=target)
        # result = self.calculate_YoY(df, target=target+'$QoQ')
        result = self.calculate_YoY(target=target+'$QoQ')
        return result

