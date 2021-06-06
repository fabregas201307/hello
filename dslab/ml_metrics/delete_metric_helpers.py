"""

Description:

- Metrics helper functions

"""

import pandas as pd


def information_coefficient(x, y, **kwargs):
    """
    Calculate information co-efficient for two vectors
    :param x:
    :param y:
    :return:
    """
    return pd.np.corrcoef(x, y)[0, 1]  # TODO: why 0, 1?
