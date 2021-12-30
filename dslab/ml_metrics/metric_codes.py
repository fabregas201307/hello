
"""

Description:

- Model evaluation codes and variable management by task type

"""

from sklearn import metrics
from dslab.mlutils import merge_dicts
from dslab.ml_models import model_codes
# from dslab.ml_metrics import metric_helpers


"""
Regression
"""


R2 = 'r2'
MEAE = 'meae'
MSE = 'mse'
MAE = 'mae'
EVS = 'evs'
INFORMATION_COEFFICIENT = 'IC'

REGRESSION_METRICS = {
    R2: metrics.r2_score,
    MEAE: metrics.median_absolute_error,
    MSE: metrics.mean_squared_error,
    MAE: metrics.mean_absolute_error,
    EVS: metrics.explained_variance_score,
#     INFORMATION_COEFFICIENT: metric_helpers.information_coefficient
}


"""
Classification
"""


F1_SCORE = 'f1'
LOG_LOSS = 'log_loss'
PRECISION = 'precision'
RECALL = 'recall'
ACCURACY = 'accuracy'
PRFS = 'prfs'
CONFUSION = 'confusion'

CLASSIFICATION_METRICS = {
    F1_SCORE: metrics.f1_score,
    LOG_LOSS: metrics.log_loss,
    PRECISION: metrics.precision_score,
    RECALL: metrics.recall_score,
    ACCURACY: metrics.accuracy_score,
    PRFS: metrics.precision_recall_fscore_support,
    CONFUSION: metrics.confusion_matrix
}


"""
Combined
"""


METRICS_BY_TYPE = {
    model_codes.REGRESSION_MODEL_TYPE: REGRESSION_METRICS,
    model_codes.CLASSIFICATION_MODEL_TYPE: CLASSIFICATION_METRICS
}

METRIC_MAP = merge_dicts(REGRESSION_METRICS, CLASSIFICATION_METRICS)

