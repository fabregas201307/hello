"""

Description:

- Build model helper functions

"""

from mltemplate.config import logger
from mltemplate.dtypes import dtype_codes
from mltemplate.ml_models import model_codes


def map_pd_to_ml_dtype(dtype):
    """
    Translate to ml dtype
    :param dtype:     (column_name, dtype) in self.data.dtypes
    :return:          str mltemplate dtype equivalent of pd dtype
    :raise:           ValueError if ml_dtype mapping error
    """
    ml_dtypes = tuple({t for t in dtype_codes.DTYPES if t in dtype})

    # If no matches, we need to add a new dtype to mltemplate
    if not ml_dtypes:
        msg = "Invalid dtype: {}; dtype not supported"
        logger.error(msg.format(dtype))
        return 'object'

    # More than one dtype should never be matched
    elif len(ml_dtypes) > 1:
        msg = "Invalid dtype: {}; matches multiple mltemplate dtypes: {}"
        raise ValueError(msg.format(dtype, ml_dtypes))

    # In this scenario there is only one matching ml_dtype; return it
    return ml_dtypes[0]


def is_dtype_numeric(dtype):
    """
    Boolean test for dtype membership in mltemplate numeric dtypes
    :param dtype:   str dtype
    :return:        bool
    """
    return dtype in dtype_codes.NUMERIC_DTYPES


def is_dtype_categorical(dtype):
    """
    Boolean test for dtype membership in mltemplate categorical dtypes
    :param dtype:   str dtype
    :return:
    """
    return dtype in dtype_codes.CATEGORICAL_DTYPES


def get_ml_model_type(dtype):
    """
    Return mlmodel type corresponding to univariate target variable
    :param dtype:   str dtype
    :return:        str model type code
    :raise:         ValueError
    """
    # Regression
    if is_dtype_numeric(dtype):
        return model_codes.REGRESSION_MODEL_TYPE

    # Classification
    elif is_dtype_categorical(dtype):
        return model_codes.CLASSIFICATION_MODEL_TYPE

    # Other:
    #   y_dtype maps to unsupported machine learning task
    msg = ";".join([
        "dtype {} not supported for available machine learning tasks",
        "regression ({}) and classification ({})"
    ])
    raise ValueError(
        msg.format(
            dtype,
            ", ".join([dtype_codes.NUMERIC_DTYPES]),
            ", ".join([dtype_codes.CATEGORICAL_DTYPES])
        )
    )

