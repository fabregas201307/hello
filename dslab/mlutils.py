
import re
import os
import sys
import glob
import pickle
import random
import sklearn
import argparse
import tempfile
import operator
import subprocess
import numpy as np  # TODO: Confirm depen
import pandas as pd
from scipy import stats
from functools import reduce
from functools import partial
from datetime import datetime
from io import StringIO, BytesIO
from collections import defaultdict


DUMMY_COL_DELIMITER = "___"


def sybase_to_data(server, table, query):
    """Create pandas DataFrame from Sybase.
    :param server: string, database server.
    :param table: string, database table.
    :param query: string, sql query.
    :return: return pandas DataFrame.
    """
    dobj = DataObject(server, autocommit=True)
    table = dobj.get_table(table)

    query = query.format(table)
    result = dobj.do_sql(query)
    return pd.DataFrame.from_records(result, columns=dobj.get_result_column_names())


def data_to_hdfs(data, client_url, hdfs_path, filename, overwrite=True, verbose=True):
    """ Move pandas.DataFrame to HDFS.
    :param data: pandas.DataFrame, the data to move.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    :param filename: string, the file name.
    :param overwrite: boolean, whether to over write exiting file.
    :param verbose: boolean, control switch for printing.
    """
    client = InsecureClient(client_url)
    data_handler = StringIO()
    data.to_csv(data_handler, index=False)

    if len(hdfs_path) == 0 or hdfs_path[-1] != '/':
        hdfs_path += '/'
    filepath = hdfs_path + filename

    data_handler.seek(0)

    with data_handler as reader, client.write(filepath, overwrite=overwrite) as writer:
        for line in reader:
            writer.write(bytes(line, 'utf8'))

        if verbose:
            print('\n Object stored in HDFS:', filepath, '\n')


def obj_to_hdfs(obj, client_url, hdfs_path, filename, overwrite=True, verbose=True):
    """ Move arbitrary Python object to HDFS.
    :param obj: the object to move.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    :param filename: string, the file name.
    :param overwrite: boolean, whether to over write exiting file.
    :param verbose: boolean, control switch for printing.
    """
    client = InsecureClient(client_url)
    obj_handler = BytesIO()
    pickle.dump(obj, obj_handler)

    if len(hdfs_path) == 0 or hdfs_path[-1] != '/':
        hdfs_path += '/'
    filepath = hdfs_path + filename

    obj_handler.seek(0)

    with obj_handler as reader, client.write(filepath, overwrite=overwrite) as writer:
        writer.write(reader.getbuffer())

        if verbose:
            print('\nStored in HDFS:', filepath, '\n')


def get_hdfs_filenames(client_url, hdfs_path):
    """Show Hadoop files and folders.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    :return: the list of file names.
    """
    client = InsecureClient(client_url)
    return client.list(hdfs_path)


def hdfs_to_data(client_url, hdfs_path, filenames, consolidated=True, low_memory=False,
                 encoding='ISO-8859-1', parse_dates=False, strip=True, index_col=None):
    """Get data from Hadoop.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    :param filenames: list, the list of file names.
    :param consolidated: boolean, whether to consolidate files into one DataFrame.
    :param low_memory: boolean, low memory usage.
    :param encoding: string, data encoding.
    :param parse_dates: boolean or list, whether to parse date column.
    :param strip: boolean, whether to strip leading and trailing white spaces. (only works when
           consolidated == True.)
    :param index_col: string, index column name.
    :raise ValueError: Can't set both filename and filenames.
    :return: one pandas DataFrame or a list of DataFrames.
    """
    client = InsecureClient(client_url)

    if len(hdfs_path) == 0 or hdfs_path[-1] != '/':
        hdfs_path += '/'

    if consolidated:
        result = None
    else:
        result = []

    for filename in filenames:
        with client.read(hdfs_path + filename, encoding=encoding) as reader:
            data = reader.read()
            data_string = StringIO(data)
            data = pd.read_csv(data_string, sep=",", parse_dates=parse_dates, low_memory=low_memory)
            if index_col is not None:
                data.set_index(index_col, inplace=True)
            if consolidated:
                result = pd.concat([result, data])
            else:
                if strip:
                    strip_data(data)
                result.append(data)

    if consolidated:
        if index_col is None:
            result.reset_index(inplace=True, drop=True)
        if strip:
            strip_data(result)
    return result


def hdfs_to_obj(client_url, hdfs_path, filename):
    """Get object from Hadoop.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    :param filename: string, file name.
    :return: the object.
    """
    client = InsecureClient(client_url)

    if len(hdfs_path) == 0 or hdfs_path[-1] != '/':
        hdfs_path += '/'

    filepath = hdfs_path + filename

    with client.read(filepath) as reader:
        model_bytes = reader.read()
        b = BytesIO(model_bytes)
        obj = pickle.load(b)

    return obj


def file_to_hdfs(unix_path, unix_sep, filename, client_url, hdfs_path):
    """Move a single file from unix to hdfs.
    :param unix_path: string, the unix path.
    :param unix_sep: string, the path separator.
    :param filename: string, the file name.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    """
    if len(unix_path) == 0 or unix_path[-1] != unix_sep:
        unix_path += unix_sep

    if len(hdfs_path) == 0 or hdfs_path[-1] != '/':
        hdfs_path += '/'

    file_input = open(unix_path + filename)
    client = InsecureClient(client_url)

    with file_input as reader, client.write(hdfs_path + filename, overwrite=True) as writer:
        for line in reader:
            writer.write(bytes(line, 'utf8'))
        print('\nFile from', unix_path, 'stored in HDFS:', hdfs_path + filename, '\n')


def files_to_hdfs(unix_path, unix_sep, client_url, hdfs_path):
    """Move files from unix to hdfs.
    :param unix_path: string, the unix path.
    :param unix_sep: string, the path separator.
    :param client_url: string, the Hadoop client url.
    :param hdfs_path: string, the Hadoop path (path to the folder).
    """
    if len(unix_path) == 0 or unix_path[-1] != unix_sep:
        hdfs_path += unix_sep

    new_files = glob.glob(unix_path + '*.csv')  # os.listdir(input_dir)

    new_files.sort()
    hdfs_files = get_hdfs_filenames(client_url, hdfs_path)

    for filepath in new_files:
        filename = os.path.basename(filepath)
        if filename not in hdfs_files:
            file_to_hdfs(unix_path, unix_sep, filename, client_url, hdfs_path)


def strip_data(data, inplace=True):
    """strip leading and trailing white spaces.
    :param data: pandas.DataFrame, data to be stripped.
    :param inplace: boolean, whether to alter data in place.
    :return: stripped data.
    """
    if inplace:
        temp = data
    else:
        temp = data.copy(deep=True)

    for column in temp.columns:
        if temp[column].dtype == 'object':
            mask = temp[column].notnull()
            temp.ix[mask, column] = temp.ix[mask, column].map(lambda x: x.strip())

    if not inplace:
        return temp


def parse_url(url):
    """Check and parse url for argparse.
    :param url: url to parse.
    :raise argparse error: invalid url.
    :return: url.
    """
    if url[:7] != 'http://':
        raise argparse.ArgumentTypeError("url should start with http://...")
    return url


def parse_date(date_string, timestamp=True):
    """Check and parse date format for argparse.
    :param date_string: date string to parse.
    :raise argparse error: invalid date.
    :param timestamp: time stamp.
    :return: parsed date.
    """
    if re.match(r'\d{1,2}-\d{1,2}-\d{2}$', date_string):
        date = datetime.strptime(date_string, '%m-%d-%y')
    elif re.match(r'\d{1,2}-\d{1,2}-\d{4}$', date_string):
        date = datetime.strptime(date_string, '%m-%d-%Y')
    elif re.match(r'\d{1,2}/\d{1,2}/\d{2}$', date_string):
        date = datetime.strptime(date_string, '%m/%d/%y')
    elif re.match(r'\d{1,2}/\d{1,2}/\d{4}$', date_string):
        date = datetime.strptime(date_string, '%m/%d/%Y')
    else:
        raise argparse.ArgumentTypeError("Invalid date input.")
    if timestamp:
        return date
    else:
        return date.date()


def parse_string(string):
    """Parse string.
    :param string: string.
    :return: parsed string.
    """
    string = string.replace("'", '"')
    return string


# def on_pc():
#     """Return True if running on a PC
#     :return: True if running on a PC
#     """
#     return sys.platform == "win32"


# def table_exists(dao, table):
#     return dao.get_db_handle().cursor().execute("select object_id('%s')" % table).fetchone()[0] is not None


# def get_tempdb_name(dao):
#     return dao.get_db_handle().cursor().execute('select db_name(@@tempdbid)').fetchone()[0]


# def get_temptable_name(dao, prefix='pytmptbl'):
#     while True:
#         tblname = '%s.guest.%s_%d' % (get_tempdb_name(dao), prefix, random.randint(1000, 10000))
#         if not table_exists(dao, tblname):
#             return tblname


# def bcp(file_path, dobj, table, delimiter=',', batch=1000):
#     user = get_user()
#     password = DataObject._get_syb_passwd()
#     (fh, err_path) = tempfile.mkstemp()
#     (server, _) = dobj._get_server_info_from_tag(dobj.db_ident)
#     env = None
#     if on_pc():
#         bcp_path = os.path.join(os.environ['SYBASE'], os.environ['SYBASE_OCS'], 'bin', 'bcp')
#     else:
#         # TODO : remove this hack once we are all on v15 clients...
#         # We need to use the v15 sybase otherwise we can BCP into DATE fields
#         sybase = get_token('SYBASE')
#         bcp_path = os.path.join(sybase, 'v15', '64bit', 'OCS-15_0', 'bin', 'bcp')
#         env = {'LD_LIBRARY_PATH': sybase + '/v15/64bit/OCS-15_0/lib:' + sybase + '/v15/64bit/OCS-15_0/lib3p',
#                'SYBASE': sybase + '/v15/64bit',
#                'SYBASE_OCS': 'OCS-15_0'}
#     command = [bcp_path, table, 'in', file_path, '-U', user,
#                '-c', '-S', server, '-e', err_path, '-b', str(batch), '-t', delimiter]
#     if on_pc():
#         command.extend(['-r', '\n'])

#     # add password after logging the command!
#     command.extend(['-P', password])
#     os.close(fh)
#     print(command)
#     try:
#         subprocess.check_call(command, env=env)
#         os.remove(err_path)
#     except subprocess.CalledProcessError as e:
#         print(e)
#         # LOG.error(e)
#         raise


def create_temp_table(file_path, dobj, sql, tmptbl_name=None, delimiter=',', batch=1000, show=False):
    """create temp table using bcp.
    :param file_path: file to load
    :param dobj: the database object
    :param tmptbl_name: name of temptable
    :param sql: sql to create temptable
    :param delimiter: table delimiter
    :param batch: bcp batch size
    :param show: whether to print the temp table
    """
    if tmptbl_name is None:
        tmptbl_name = get_temptable_name(dobj)

    if table_exists(dobj, tmptbl_name):
        dobj.do_sql("drop table %s" % tmptbl_name)

    dobj.do_sql(sql % (tmptbl_name, tmptbl_name))

    bcp(file_path, dobj, tmptbl_name, delimiter, batch)

    if show:
        rows = dobj.do_sql('select * from %s' % tmptbl_name)
        print(pd.DataFrame.from_records(rows, columns=dobj.get_result_column_names()))


def import_clients():
    """import client lists with client: client_server pair
    :return: clients as dictionary
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../static/client_db.txt'))
    df.set_index('CLIENT', drop=True, inplace=True)
    return df.to_dict()['VALUE']


# def to_except_monintor(pred, data):
#     """write to except monintor
#     :param pred: pd.Series, prediction
#     :param data: pd.DataFrame, data used to identify records
#     """
#     pass


def update_one_row(pred, client, cusip, fund_code, package, run_date, unique_id, server_id, priority):
    """update one row in exception monintor
    :param pred: prediction
    :param client: client
    :param cusip:
    :param fund_code:
    :param package:
    :param run_date:
    :param unique_id: to identify the row
    :param server_id:
    :param priority:
    """

    comment = 'Can be verified.' if pred == 'V' else 'Suggest to investigate.'

    message = {}
    message["data"] = {}
    message["data"]["changes_map"] = {}
    message["data"]["changes_map"]["0"] = []
    submessage = {}
    submessage["client"] = client
    submessage["cusip"] = cusip
    submessage["fund_code"] = fund_code
    submessage["package"] = package
    submessage["process"] = "pms"
    submessage["run_date"] = run_date
    submessage["unique_id"] = unique_id
    submessage["update_fields"] = {}
    submessage["update_fields"]["approval_reason"] = "Data Science Model"
    submessage["update_fields"]["comments"] = comment

    message["data"]["changes_map"]["0"].append(submessage)
    message["package"] = package
    message["process"] = "pms"
    message["status_time"] = "11/6/2015 22:05:43.000"  # datetime.today().strftime("%x %X")
    message["type"] = "FIELD_UPDATE"
    message["user_id"] = "bihan"

    #source_id = int(str(server_id) + '03')  # source id for the exception monitor
    #bmso = bms.BMS()
    #m = bms.Message()
    #m.init(bms.SemaphoreMsgBody(message, brmap.Encode.BINARY), source_id)
    #m._header._timezone_name = "UTC"
    #m._header._time_created = int(datetime.utcnow().timestamp())
    #m._header._msg_subtype = server_id
    #m._header._msg_priority = priority

    try:
        print(m)
        print("sending message!!!")
        #r = bmso.send(m, compress_body=True)  # returns list of response messages
        print("message sent. Waiting for reply")
        #for i in r:
        #    print(i)
    except Exception as e:
        print(str(e))
    finally:
        # It's best practice to always close your connection.
        #bmso.close()
        print("bmso.close")


def df_to_json(df):
    """Convert DataFrame to Json
    :param df: data frame to be converted.
    :return: the json file.
    """

    return df.to_json(orient='index')


def csv_to_json_file(filepath, recode_dict, output_columns, savepath=None, parse_dates=False):
    """convert csv to json file, and return json data
    :param filepath: path to get csv
    :param recode_dict: rename some columns
    :param output_columns: columns to select
    :param savepath: path to save json file
    :param parse_dates: same as pandas parse_dates
    :return: json data
    """
    df = pd.read_csv(filepath, low_memory=False, encoding='ISO-8859-1', thousands=',', parse_dates=parse_dates)
    df.rename(columns=recode_dict, inplace=True)
    df = df.reindex(columns=output_columns)
    json = df.to_json(orient='index')
    if savepath is not None:
        ff = open(savepath, 'w')
        ff.write(json)
        ff.close()
    return json


def set_python_path(py_lib_dir, req_file):
    """read requirements.txt file (if it exists) and construct python path for script
    :param py_lib_dir: py lib dir
    :param req_file: req file
    :return: required python path to run script
    """

    if not os.path.exists(req_file):
        return ''
    f = None
    elements = []
    try:
        f = open(req_file)
        for line in f:
            line = line.strip()
            if len(line) == 0 or line.startswith('#') or line.find('==') == -1:
                continue
            (pkg_name, pkg_ver) = line.split('==')
            d = os.path.join(py_lib_dir, pkg_name, pkg_ver)
            elements.append(d)
            sys.path.append(d)
    except Exception as e:
        print(e)
    finally:
        if f is not None:
            f.close()
    return ':'.join(elements)


def identity(x):
    """
    Returns x
    :param x:   obj
    :return:    x
    """
    return x


def is_none(x):
    """
    :param x:   obj
    :return:    True if obj is None, else False
    """
    return x is None


def is_not_none(x):
    """
    :param x:   obj
    :return:    False if obj is None, else True
    """
    return not is_none(x)


def merge_dict(a, b):
    """
    :param a:   dict
    :param b:   dict
    :return:    a updated with b's (k, v) associations
    """
    _a = a.copy()
    _a.update(b)
    return _a


def merge_dicts(*dicts):
    """
    Merge right to left; reverse iter as input to get left to right
    :param dicts:   iter of dicts
    :return:        union of all dict in dicts overwritten from right to left
    """
    return reduce(
        lambda x, y: merge_dict(x, y),
        dicts,
        dict()
    )


def dict_filter_by_key(d, lookup):
    """
    :param d:       dictionary
    :param lookup:  iter of obj corresponding to keys
    :return:        d filtered to include only items whose key eq lookup
    """
    return {k: v for k, v in d.items() if k == lookup}


def dict_filter_by_keys(d, lookups):
    """
    :param d:       dictionary
    :param lookups: iter of obj corresponding to keys
    :return:        d filtered to include only items whose key in lookups
    """
    return {k: v for k, v in d.items() if k in lookups}


def dict_filter_by_value(d, lookup):
    """
    :param d:       dictionary
    :param lookup:  iter of obj corresponding to values
    :return:        d filtered to include only items whose value eq lookup
    """
    return {k: v for k, v in d.items() if v == lookup}


def dict_filter_by_lookup_in_value(d, lookup):
    """
    :param d:       dictionary
    :param lookup:  iter of obj corresponding to values
    :return:        d filtered to include only items whose value eq lookup
    """
    return {k: v for k, v in d.items() if lookup in v}


def dict_filter_by_values(d, lookups):
    """
    :param d:       dictionary
    :param lookups: iter of obj corresponding to values
    :return:        d filtered to include only items whose value in lookups
    """
    return {k: v for k, v in d.items() if v in lookups}


def dict_lookup(d, o, m):
    """
    :param d:       dictionary
    :param o:       obj to be looked up in d
    :param m:       method to be used to lookup o in d
    :return:
    """
    return m(d, o)


def write_obj_to_file(path, fn, obj):
    """
    :param path: path to file
    :param fn:   filename
    :param obj:  obj to be cast as str and written to file
    """
    with open(os.path.join(path, fn), 'w') as f:
        f.write(str(obj))


def roc_optimize_point(all_points, slope):
    """
    :param all_points:  pandas DataFrame, each row is one point in 2D space
    :param slope:       slope of tangent line
    :return:            list type, 2D point [x, y]
    """
    if(all_points.shape[0] < 1):
        return ([None] * 2)
    _ = all_points.iloc[0, :].tolist()
    scores = [all_points.iloc[i, 1] - slope * all_points.iloc[i, 0] for i in range(0, all_points.shape[0])]
    scores_np = np.array(scores)
    result = all_points.iloc[scores_np.argmax(), :].tolist()
    return (result)


def compose(fns):
    """
    Compose arbitrary number of 1-ary functions
    :param fns:     iter of fns for composition; f -> g -> h;
    :return:        func ... h(g(f(x)))
    """
    return reduce(
        lambda f, g: lambda x: g(f(x)),
        fns,
    )


def str_format(s, *args, **kwargs):
    """
    Returns formatted string
    :param s:       str
    :param args:    tuple args
    :param kwargs:  dict args
    :return:        str formatted
    """
    return s.format(*args, **kwargs)


def tautology(*args, **kwargs):
    """
    Always returns True
    :param args:    ...
    :param kwargs:  ...
    :return:        True
    """
    return True


def falsum(*args, **kwargs):
    """
    Always returns False
    :param args:    ...
    :param kwargs:  ...
    :return:        False
    """
    return False


def logical_complement(x):
    """
    Return not bool(x)
    :param x:   obj
    :return:    not bool(x)
    """
    return not bool(x)


def null(*args, **kwargs):
    """
    Always return None
    :param args:    ...
    :param kwargs:  ...
    :return:        None
    """
    return None


def exception(*args, m=None, e=Exception, **kwargs):
    """
    Always return an Exception of type e
    :param args:    ...
    :param e:       Exception subclass
    :param m:       str else None
    :param kwargs:  ...
    :raise:         Exception(m)
    """
    raise e(m)


def flatten_lists(*ls):
    """
    Flatten a list of lists into one list
    :param ls:  list of lists of depth 0
    :return:    list
    """
    return reduce(
        lambda x, y: x + list(y),
        ls,
        []
    )


def list_of_dict_to_dict_of_list(l):
    """
    Convert a list of dict to a dict of lists on common key
    Does not assume that the lists in the resulting dict are not ragged
    :param l:   list of dicts
    :return:    dict of lists
    """
    dd = defaultdict(list)
    item_gen = (
        (k, v)
        for d in l
        for k, v in d.items()
    )
    for k, v in item_gen:
        dd[k].append(v)
    return dict(dd)


def adjusted_coefficient_of_determination(r_square, n, p):
    """
    Return adjusted coefficient of determination
    :param r_square:    R2 score from scikitlearn ensemble or linear model
    :param n:           int sample size
    :param p:           int total number of explanatory variables in the model not including constant terms
    :return:            int Adjusted R2
    """
    print(r_square, n, p)
    return 1 - (1 - r_square) * (n - 1) / (n - p - 1)


def is_dummy_col(col_name):
    """
    Dummy column name predicate
    :param col_name:    str column name
    :return:            bool
    """
    return DUMMY_COL_DELIMITER in col_name


def get_dummy_category(col_name):
    """
    Parses out category name from dummy column
    :param col_name:    str dummy column name
    str = category + DUMMY_COL_DELIMTER + value delimited dummy col names
    :return:            str category
    """
    return col_name.split(DUMMY_COL_DELIMITER)[0]


def get_categorized_column_names(df):
    """
    Get a list of "categorized" names for a list of columns in dataframe for groupby operations
    :param df:  pd.DataFrame
    :return:    list of str of col_name for non dummy and category name for dummies
    """
    return [
        get_dummy_category(col) if is_dummy_col(col) else col
        for col in df.columns
    ]


# def _remove_dummy_categoricals(ml_model, xtrain, ytrain):
#     """
#     Remove one of each of a pair of dummy variables
#     :param ml_model:
#     :param xtrain:
#     :param ytrain:
#     :return:
#     """
#     original_columns = xtrain.columns
#     f_scores, p_values = sklearn.feature_selection.f_regression(
#         X=xtrain[ml_model.dummy_indep_cols],
#         y=ytrain,
#         center=True
#     )
#     f_regression_df = dict(zip(('f_score', 'p_value'), (f_scores, p_values)))
#     category_col_name = '_dummy_category'
#     category_labels = pd.DataFrame({'_dummy_category': get_categorized_column_names(xtrain)})
#     categorized = pd.DataFrame.concat([xtrain, f_regression_df, category_labels], axis=1)
#     grouped_by_category = categorized.groupby(category_col_name)
#     for group in grouped_by_category:
#         min_p_value = group['p_value'].min()
#         min_label = group.which(p_value == min_p_value)


def linear_model_p_stats(ml_model, residuals, xtrain, ytrain):
    """
    Caculate t-statistics and p-values for a linear model
    :param ml_model:    MLModel
    :param residuals:   array residuals
    :param xtrain:      pd.DataFrame xtrain
    :param ytrain:      pd.DataFrame ytrain
    """
    try:
        sse = (residuals ** 2) / float(xtrain.shape[0] - xtrain.shape[1])
        se = np.array([
            np.sqrt(np.diagonal(sse[i] * np.linalg.inv(np.dot(xtrain.T, xtrain))))
            for i in range(sse.shape[0])
        ])
        ml_model.t_stats = ml_model.coef_ / se
        # Two-sided test
        ml_model.p_values = 2 * (
            1 - stats.t.cdf(np.abs(ml_model.t_stats), xtrain.shape[0] - xtrain.shape[1])
        )
    except np.linalg.linalg.LinAlgError:
        f_scores, p_values = sklearn.feature_selection.f_regression(
            X=xtrain[ml_model.dummy_indep_cols],
            y=ytrain,
            center=True
        )


def obj_join_composer(c, a, b):
    """
    Take two items a, b; insert c between them
    :param a:   obj
    :param b:   obj
    :param c:   obj used to join a, b
    :return:    tuple
    """
    return a + (c, b)


def obj_join(c, *objs):
    """
    Delimit a sequence with an object c
    :param c:       obj used to delimit objs sequence
    :param objs:    tuple of objects to be delimited
    :return:        tuple of delimited objects
    """
    return reduce(
        partial(obj_join_composer, c),
        objs
    )


def obj_envelop(c, *objs):
    """
    Envelop a sequence of objs with an eveloping object c
    :param c:       obj used to envelop objs sequence
    :param objs:    tuple of objs
    :return:
    """
    return reduce(
        operator.add,
        ((c,), objs, (c,)),
        tuple()
    )


def f_envelop(f, g):
    """
    Envelop a function or procedure with two other functions
    :param f:   function to be enveloped
    :param g:   function to envelop
    :return:    function
    """
    return compose((g, f, g))


def f_join_composer(h, f, g):
    """
    Compose two functions together via an intermediary
    :param h:   function intermediary used to join f and g
    :param f:   function
    :param g:   function
    :return:    function f o h o g
    """
    return compose((f, h, g))


def f_join(g, *fs):
    """
    Functionally compose a sequence of functions via an intermediary function g
    :param f:
    :param g:
    :return:
    """
    return reduce(
        partial(f_join_composer, g),
        fs
    )


def is_platform_linux():
    """
    Is the platform linux type?
    :return:    bool
    """
    return sys.platform.startswith('linux')


def is_platform_windows():
    """
    Is the platform windows type?
    :return:    bool
    """
    return sys.platform.startswith('win')


def is_platform_mac():
    """
    Is the platform mac type?
    :return:    bool
    """
    return sys.platform.startswith('darwin')

