"""

Description:

- mltemplate.mldata codes and variable management

"""

import operator
from functools import reduce


"""
ML dtypes
"""


# TODO: Include byte length specification combinations with types below?
# NOTE: pd vs. mltemplate dtypes: We don't care about byte specification, only type interface

# Logical

BOOLEAN = 'bool'

# Numeric

INT = 'int'
FLOAT = 'float'
COMPLEX = 'complex'

# Categorical

OBJECT = 'object'
CATEGORY = 'category'

# Time

DATETIME = 'datetime'
TIMEDELTA = 'timedelta'

# Collections

LOGICAL_DTYPES = [BOOLEAN]
NUMERIC_DTYPES = LOGICAL_DTYPES + [INT, FLOAT, COMPLEX]
CATEGORICAL_DTYPES = [OBJECT, CATEGORY]
TIME_DTYPES = [DATETIME, TIMEDELTA]

DTYPES = list(
    set(
        reduce(
            operator.add,
            (
                LOGICAL_DTYPES,
                NUMERIC_DTYPES,
                CATEGORICAL_DTYPES,
                TIME_DTYPES
            ),
        )
    )
)