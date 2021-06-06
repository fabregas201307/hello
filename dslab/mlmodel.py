
import os
import re
import sys
import html
import copy
import inspect
import logging
import numpy as np
import pandas as pd
from scipy import stats
from functools import reduce
from collections import OrderedDict

import sklearn
from sklearn import tree
from sklearn import metrics as metrics_
from sklearn.decomposition import FastICA, PCA
from sklearn.linear_model.base import LinearModel
from sklearn.model_selection import cross_val_score

from dslab.mlutils import merge_dict
from dslab.ml_metrics import metric_codes
from dslab.config import init_logger, log_fun_info
from dslab.ml_models import model_codes, model_helpers

# GRAPHVIZ_UNIX_DIR = '/usr/local/graphviz-2.28.0/bin/'
# sys.path.append("/usr/local/graphviz/bin")
# logger = init_logger(__name__, logging.WARN)

# METRIC_MAP = {'r2': 'r2_score',
#               'meae': 'median_absolute_error',
#               'mse': 'mean_squared_error',
#               'mae': 'mean_absolute_error',
#               'evs': 'explained_variance_score',
#               'f1': 'f1_score',
#               'log_loss': 'log_loss',
#               'precision': 'precision_score',
#               'recall': 'recall_score',
#               'accuracy': 'accuracy_score',
#               'prfs': 'precision_recall_fscore_support',
#               'confusion': 'confusion_matrix'}

CROSS_VALIDATION_METRIC_MAP = {'r2': 'r2',
                               'meae': 'median_absolute_error',
                               'mse': 'mean_squared_error',
                               'mae': 'mean_absolute_error',
                               'f1': 'f1',
                               'log_loss': 'log_loss',
                               'precision': 'precision',
                               'recall': 'recall',
                               'accuracy': 'accuracy'}


class MLModel:
    """
    Define the model, train the model and test the model
    **Class Attributes**:
    :py:attr:`self.model`: the machine learning model.
    :py:attr:`self.models`: a dict containing a list of models with their names.
    """
    def __init__(self, model=None, models=None):
        """
        Constructor.
        :param model:   sklearn model
        :param models:  list of sklearn models
        """
        # TODO: model vs. models; let's revisit when we have a clearly defined use case
        #       for multiple models.
        #       In the meantime, please keep MLModel instances as 1:1 with an sklearn model obj
#         log_fun_info(logger)
        self.model = model
        self.models = models
        self.model_type = None
        self.indep_cols = None

    def set_frp_dp(self, frp, dp):
        """
        Set fix rule predictor
        :param frp:     Class, the fix rule predictor.
        :param dp:      Class, the data processor.
        """
        # TODO: Is this now deprecated?
        self.frp = frp
        self.dp = dp

    def _check_model_type(self):
        """
        model type validation
        :return:
        :raise:     ValueError
        """
        if self.model_type is None:
            raise ValueError("Please define a regressor or classifier!")
        elif self.model_type not in model_codes.MODEL_TYPES:
            msg = "Model type '{}' is invalid; does not match any available model types {}!"
            raise ValueError(msg.format(self.model_type, ", ".join(model_codes.MODEL_TYPES)))

    def define_regressor(self, model_name=model_codes.DECISION_TREE_REGRESSOR, **kwargs):
        """
        Initiate a given regressor model type
        :param model_name:  str the name of the regressor
                    all:    All Regressors
                    ols:    Ordinary Least Square
                    ridge:  Ridge Regressor
                    lasso:  Lasso Regressor
                    enet:   Elastic Net Regressor
                    bayes:  Bayesian Ridge Regressor
                    ransac: RANSAC (robust regression)
                    rfrgr:     Random Forest Regressor
                    ert:    Extremely Randomized Trees
                    ab:     Ada Boost Regressor
                    gbrgr:     Gradient Boosting Regressor
                    bag:    Bagging
                    svr:    Support Vector Machine Regressor
                    dtrgr:     Decision Tree Regressor
                    knnrgr:    KNeighbors Regressor
        :param kwargs:      dict single regressor parameters; elif 'all' option, params should be split by model
        :raise ValueError:  invalid model_name passed
        """
#         log_fun_info(logger, log_arg=True)

        # TODO: Refactor
        #     You chose 'all' but now can optionally select partial params through kwargs
        #     What protection is there from mixing single and multi-models?
        #     Multi-model method should be different aggregator of single-model methods
        if model_name == model_codes.RUN_ALL_REGRESSORS:
            self.model = None
            self.models = {
                name: constructor()
                for name, constructor in model_codes.REGRESSION_MODELS.items()
            }
            for model_name in kwargs:
                self.models[model_name].set_params(**kwargs[model_name])
        elif model_name in model_codes.REGRESSION_MODELS:
            self.models = None
            self.model = model_codes.REGRESSION_MODELS[model_name]()
            self.model.set_params(**kwargs)
        else:
            msg = "{} is an invalid regressor"
            raise ValueError(msg.format(model_name))

        self.model_type = model_codes.REGRESSION_MODEL_TYPE

    def define_classifier(self, model_name=model_codes.RANDOM_FOREST_CLASSIFIER, **kwargs):
        """
        Initiate the classifier
        :param model_name:  str, the name of the classifier.
                     all:   all classifiers
                     rf:    Random Forest Classifier
                     ert:   Extremely Randomized Trees
                     ab:    Ada Boost Classifier
                     gb:    Gradient Boosting Classifier
                     bag:   Bagging
                     lsvc:  Linear SVC
                     svm:   Support Vector Machine
                     dt:    Decision Tree Classifier
                     lg:    Logistic Regression
                     lda:   Linear Discriminant Analysis
                     qda:   Quadratic Discriminant Analysis
                     gnb:   Gaussian Naive Bayes
                     bnb:   Bernoulli Naive Bayes
                     mnb:   Multinomial Naive Bayes
                     knn:   KNeighbors Classifier
        :param kwargs:      use dictionary or key word arguments for classifier parameters
        :raise:             ValueError
        """
#         log_fun_info(logger, log_arg=True)

        if model_name == model_codes.RUN_ALL_CLASSIFIERS:
            self.model = None
            self.models = {
                name: constructor()
                for name, constructor in model_codes.CLASSIFICATION_MODELS.items()
            }
            for model_name in kwargs:
                self.models[model_name].set_params(**kwargs[model_name])
        elif model_name in model_codes.CLASSIFICATION_MODELS:
            self.models = None
            self.model = model_codes.CLASSIFICATION_MODELS[model_name]()
            self.model.set_params(**kwargs)
        else:
            msg = "{} is an invalid classifier"
            raise ValueError(msg.format(model_name))

        self.model_type = model_codes.CLASSIFICATION_MODEL_TYPE

    def define_model(self, model_type, model_name, **kwargs):
        """
        Define a model of a given type by name
        :param model_type:  str model type
        :param model_name:  str model name
        :param kwargs:      dict kwargs for constructor
        :raise:             ValueError
        """
        constructors_by_type = {
            model_codes.REGRESSION_MODEL_TYPE: self.define_regressor,
            model_codes.CLASSIFICATION_MODEL_TYPE: self.define_classifier
        }

        constructor = constructors_by_type.get(model_type, None)

        if constructor is None:
            msg = 'Invalid model_type {}; Valid model_types {}'
            raise ValueError(msg.format(model_type, ", ".join(model_codes.MODEL_TYPES)))

        constructor(model_name, **kwargs)

    def split_random(self, x, y, train_ratio=0.8, random_state=None, check_index=False):
        """
        Split data into testing and training randomly
        Make sure x and y have the same indexes
        :param x: DataFrame, feature matrix.
        :param y: Series, target vector.
        :param train_ratio: float, the proportion used for the training set. The remaining will be
                            used for testing set.
        :param random_state: random seed.
        :param check_index: bool, check if indices of x and y match
        :raise ValueError:
        :return: xtrain, ytrain, xtest, ytest
        """
        if check_index:
            if not np.array_equal(x.index, y.index):
                raise ValueError("Indices of x and y don't match!")
        elif len(x) != len(y):
            raise ValueError("x and y don't have the same length!")

        if not x.index.is_unique or not y.index.is_unique:
            x = x.reset_index(drop=True)
            y = y.reset_index(drop=True)

#         log_fun_info(logger)

        if random_state is not None:
            np.random.seed(random_state)

        new_index = np.random.permutation(x.index)
        xtemp = x.reindex(new_index)
        ytemp = y.reindex(new_index)

        xtrain = xtemp[:int(len(xtemp) * train_ratio)]
        xtest = xtemp[int(len(xtemp) * train_ratio):]
        ytrain = ytemp[:int(len(ytemp) * train_ratio)]
        ytest = ytemp[int(len(ytemp) * train_ratio):]
        return xtrain, ytrain, xtest, ytest

    def split_by_value(self, x, y, ref, test_value, check_index=False):
        """
        Split data into testing and training by test set value
        Make sure x, y and ref have the same indices
        :param x: DataFrame, feature matrix.
        :param y: Series, target vector.
        :param ref: Series, the column to split on (as the reference column).
        :param test_value: the value for testing data.
        :param check_index: bool, check if indices of x, y and ref match.
        :raise ValueError:
        :return: xtrain, ytrain, xtest, ytest
        """
        if check_index:
            if not np.array_equal(x.index, y.index):
                raise ValueError("Indices of x and y don't match!")
            if not np.array_equal(x.index, ref.index):
                raise ValueError("Indices of x and ref don't match!")
        else:
            if len(x) != len(y):
                raise ValueError("x and y don't have the same length!")
            if len(x) != len(ref):
                raise ValueError("ref does not have the same length as x or y!")

#         log_fun_info(logger)

        xtrain = x[ref != test_value]
        xtest = x[ref == test_value]
        ytrain = y[ref != test_value]
        ytest = y[ref == test_value]
        return xtrain, ytrain, xtest, ytest

    def split_by_values(self, x, y, ref, train_values=None, test_values=None, check_index=False):
        """
        Split data into testing and training by subset of values
        Make sure x, y and ref have the same indices
        :param x: DataFrame, feature matrix.
        :param y: Series, target vector.
        :param ref: Series, the column to split on (as the reference column).
        :param train_values: list, the set of values that training set belong to, if None then
                             test_values will be used (train_values and test_values can't both be None).
        :param test_values: list, the set of values that testing set belong to, if None then
                            train_values will be used (train_values and test_values can't both be None).
        :param check_index: bool, check if indices of x, y and ref match.
        :raise ValueError:
        :return: xtrain, ytrain, xtest, ytest
        """
        if check_index:
            if not np.array_equal(x.index, y.index):
                raise ValueError("Indices of x and y don't match!")
            if not np.array_equal(x.index, ref.index):
                raise ValueError("Indices of x and ref don't match!")
        else:
            if len(x) != len(y):
                raise ValueError("x and y don't have the same length!")
            if len(x) != len(ref):
                raise ValueError("ref does not have the same length as x or y!")

        if train_values is None and test_values is None:
            raise ValueError("'train_values' and 'test_values' can't be both None.")

#         log_fun_info(logger)

        if train_values is None:
            train_values = np.setdiff1d(ref.unique(), test_values)
        if test_values is None:
            test_values = np.setdiff1d(ref.unique(), train_values)

        train_values = list(train_values)
        test_values = list(test_values)

        xtrain = x[ref.map(lambda x: x in train_values)]
        xtest = x[ref.map(lambda x: x in test_values)]
        ytrain = y[ref.map(lambda x: x in train_values)]
        ytest = y[ref.map(lambda x: x in test_values)]
        return xtrain, ytrain, xtest, ytest

    def split_by_values_with_threshold(self, x, y, ref, threshold=0.7, check_index=False):
        """
        Split data into training and testing by threshold percentage on ascending order
        Make sure x, y and ref have the same indices
        :param x: DataFrame, feature matrix.
        :param y: Series, target vector.
        :param ref: Series, the column to split on (as the reference column).
        :param threshold: float, values below threshold go to train, otherwise go to test.
        :param check_index: bool, check if indices of x, y and ref match.
        :raise ValueError:
        :return: xtrain, ytrain, xtest, ytest
        """
        if check_index:
            if not np.array_equal(x.index, y.index):
                raise ValueError("Indices of x and y don't match!")
            if not np.array_equal(x.index, ref.index):
                raise ValueError("Indices of x and ref don't match!")
        else:
            if len(x) != len(y):
                raise ValueError("x and y don't have the same length!")
            if len(x) != len(ref):
                raise ValueError("ref does not have the same length as x or y!")

        if threshold > 1 or threshold < 0:
            raise ValueError("'threshold has to be between 0 and 1")

#         log_fun_info(logger)
        cut_point = ref.quantile(q=threshold)

        xtrain = x[ref <= cut_point]
        xtest = x[ref > cut_point]
        ytrain = y[ref <= cut_point]
        ytest = y[ref > cut_point]
        return xtrain, ytrain, xtest, ytest

    def up_sample(self, xtrain, ytrain, neg_val=None, fold=1, check_index=False):
        """
        Brutal force up sampling the negative class
        Make sure xtrain and ytrain have the same indexes
        :param xtrain: DataFrame, the feature matrix.
        :param ytrain: Series, the target vector.
        :param neg_val: value for the negative class. If None, then neg_val will be the
            derived from value counts.
        :param fold: int, the number of folds to up sample.
        :param check_index: bool, check if indices of xtrain and ytrain match.
        :raise ValueError:
        :return: xtrain, ytrain
        """
        if check_index:
            if not np.array_equal(xtrain.index, ytrain.index):
                raise ValueError("Indices of xtrain and ytrain don't match!")
        elif len(xtrain) != len(ytrain):
            raise ValueError("xtrain and ytrain do not have the same length!")

#         log_fun_info(logger)

        vc = ytrain.value_counts()
        if len(vc) > 2:
            raise ValueError("Not a two class problem!")
        if neg_val is None:
            neg_val = vc.index[1]
        xtrain_up_sample = xtrain.ix[ytrain == neg_val]
        ytrain_up_sample = ytrain.ix[ytrain == neg_val]
        for _ in range(fold):
            xtrain = pd.concat([xtrain, xtrain_up_sample], axis=0)
            ytrain = pd.concat([ytrain, ytrain_up_sample], axis=0)
        return xtrain, ytrain

    def fit_model(self, xtrain, ytrain, indep_cols, sample_weights=None, check_index=False):
        """
        Fit the model
        Make sure xtrain, ytrain and sample_weights have the same indices
        :param xtrain: DataFrame, training data.
        :param ytrain: Series, the target vector.
        :param indep_cols: list, the set of column names to build the model.
        :param sample_weights: Series, column to use as sample weights to fit the model.
        :param check_index: bool, check if indices of xtrain, ytrain and sample_weights match.
        :raise ValueError:
        """
        if check_index:
            if not np.array_equal(xtrain.index, ytrain.index):
                raise ValueError("Indices of xtrain and ytrain don't match!")
            if (sample_weights is not None) and (not np.array_equal(xtrain.index, sample_weights.index)):
                raise ValueError("Indices of xtrain and sample_weights don't match!")
        else:
            if len(xtrain) != len(ytrain):
                raise ValueError("xtrain and ytrain do not have the same length!")
            if sample_weights is not None and len(ytrain) != len(sample_weights):
                raise ValueError("sample_weights does not have the same length as xtrain/ytrain!")

#         log_fun_info(logger)

        if not hasattr(ytrain, 'name') or ytrain.name is None:
            self.target_name = 'y'
        else:
            self.target_name = ytrain.name

        dummy_indep_cols = [col for col in xtrain.columns if col.split("___")[0] in indep_cols]
        self.indep_cols = indep_cols
        self.dummy_indep_cols = dummy_indep_cols

        if sample_weights is not None:
            if 'sample_weight' not in inspect.getargspec(self.model.fit).args:
                raise ValueError("The model fit method has no argument 'sample_weight'!")

            self.model.fit(xtrain[dummy_indep_cols], np.ravel(ytrain), sample_weights)
        else:
            self.model.fit(xtrain[dummy_indep_cols], np.ravel(ytrain))

    def predict(self, xtest, return_proba=False, pos_label=None, min_proba_for_pos=None):
        """
        Make Predictions
        :param xtest: panda Data Frame, testing data.
        :param return_proba: bool, whether to write predicted probability into the output file.
                             Only applies to classification problem.
        :param pos_label: positive label. Only applies to two-class problem.
        :param min_proba_for_pos: float, probability cut-off for positive class. Only applies for two-
                                  class problems.
        :raise ValueError:
        :return: if not return_proba, returns a panda Series with predictions; else, returns both
                 prediction and predicted probabilities.
        """
        if self.model_type is None:
            raise ValueError('Model is not defined!')

        if return_proba and not hasattr(self.model, 'predict_proba'):
            raise ValueError("The model has no method named 'predict_proba(X)'!")

        if not hasattr(self, 'dummy_indep_cols'):
            raise ValueError("You haven't fit any model yet!")

#         log_fun_info(logger)

        xtest = xtest.reindex(columns=self.dummy_indep_cols, fill_value=0)

        if self.model_type == model_codes.REGRESSION_MODEL_TYPE:
            predicted = pd.Series(self.model.predict(xtest), xtest.index, name='Predicted ' + self.target_name)
            return predicted
        elif self.model_type == model_codes.CLASSIFICATION_MODEL_TYPE:
            classes = self.model.classes_
            n_class = len(classes)

            if n_class == 2 and min_proba_for_pos is not None and pos_label is not None:

                if pos_label not in classes:
                    raise ValueError('{} is not a valid positive label'.format(pos_label))

                predicted_proba = pd.DataFrame(self.model.predict_proba(xtest), columns=classes, index=xtest.index)

                neg_label = list(filter(lambda x: x != pos_label, classes))[0]
                predicted = predicted_proba[pos_label].map(lambda x: pos_label if x >= min_proba_for_pos
                                                           else neg_label)
                predicted.name = 'Predicted ' + self.target_name
            else:
                predicted = pd.Series(self.model.predict(xtest), name='Predicted ' + self.target_name, index=xtest.index)
                if return_proba:
                    predicted_proba = pd.DataFrame(self.model.predict_proba(xtest), index=xtest.index, columns=classes)

            if return_proba:
                if n_class == 2 and min_proba_for_pos is not None and pos_label is not None:
                    predicted_proba.drop([neg_label], axis=1, inplace=True)
                predicted_proba.columns = ['Confidence: ' + str(x) for x in list(predicted_proba.columns)]
                return predicted, predicted_proba
            else:
                return predicted
        else:
            raise ValueError("Model type '{}' is invalid!".format(self.model_type))

    def get_metric(self, ytrue, ypred, metric, check_index=False, **kwargs):
        """
        Generates sklearn metric for a single metric code
        :param ytrue:           Series, labelled testing data
        :param ypred:           Series, predicted values
        :param metric:          metric code
        Options include:
            regression:
                r2:             r2_score (default)
                meae:           median_absolute_error
                mse:            mean_squared_error
                mae:            mean_absolute_error
                evs:            explained_variance_score
            classification:
                accuracy:       accuracy_score (default)
                f1:             f1_score
                log_loss:       log_loss
                precision:      precision_score
                recall:         recall_score
                prfs:           precision_recall_fscore_support
                confusion:      confusion_matrix
        :param check_index:     bool, check if indices of ytrue and ypred match
        :raise ValueError:      Multiple validation conditions defined below
        :return:                Return the performance metric value
        """
        if check_index:
            if not np.array_equal(ytrue.index, ypred.index):
                raise ValueError("Indices of ytest and ypred don't match!")

        # NOTE: No need for this extra step; evaluating the metric will already do this
        elif len(ytrue) != len(ypred):
            raise ValueError("ytest and ypred don't have the same length!")

        self._check_model_type()

        if metric not in metric_codes.METRICS_BY_TYPE[self.model_type]:
            raise ValueError("Invalid metric '{}' for model type '{}'!".format(metric, self.model_type))

        return metric_codes.METRICS_BY_TYPE[self.model_type][metric](ytrue, ypred, **kwargs)

    def get_metrics(self, ytrue, ypred, metric_list=None, check_index=False, **kwargs):
        """
        Generates sklearn metrics for a multiple metric codes
        :param ytrue:           Series, labelled testing data
        :param ypred:           Series, predicted values
        :param metric_list:     list of metric codes
        :param check_index:     bool, check if indices of ytrue and ypred match
        :return:                dict of performance metric value by metric code names
        :raise ValueError:      Multiple validation conditions defined below
        """
        if check_index:
            if not np.array_equal(ytrue.index, ypred.index):
                raise ValueError("Indices of ytest and ypred don't match!")

        # NOTE: No need for this extra step; evaluating the metric will already do this
        elif len(ytrue) != len(ypred):
            raise ValueError("ytest and ypred don't have the same length!")

        self._check_model_type()

        return reduce(
            lambda x, metric: merge_dict(
                x,
                {metric: self.get_metric(ytrue, ypred, metric, **kwargs.get(metric, {}))}
            ),
            metric_list,
            {}
        )

    def eval_model(self, ytrue, ypred, metric_list=None, check_index=False, **kwargs):
        """
        Evaluate model performance
        Generates sklearn metrics
        :TODO:  Deprecate
        :param ytrue:           Series, testing data.
        :param ypred:           Series, predicted values.
        :param metric_list:     str or list of metric codes
        :param check_index:     bool, check if indices of ytrue and ypred match.
        :raise ValueError:      Multiple validation conditions defined below
        :return:                Return the performance score(s)
        """
        if check_index:
            if not np.array_equal(ytrue.index, ypred.index):
                raise ValueError("Indices of ytest and ypred don't match!")
        elif len(ytrue) != len(ypred):
            raise ValueError("ytest and ypred don't have the same length!")

        if metric_list is None:
            if self.model_type is None:
                raise ValueError("Define a model first!")
            elif self.model_type == model_codes.REGRESSION_MODEL_TYPE:
                metric_list = [metric_codes.R2]
            elif self.model_type == model_codes.CLASSIFICATION_MODEL_TYPE:
                metric_list = [metric_codes.ACCURACY]
            else:
                raise ValueError("Model type '{}' is invalid!".format(self.model_type))

#         log_fun_info(logger)

        # NOTE: Preserved for backwards compatibility; Can use get_metric with string also
        if isinstance(metric_list, str):
            metric_list = [metric_list]

        # NOTE: Preserved for backwards compatibility; Can use get_metric with string also
        if len(metric_list) < 2:
            return self.get_metric(ytrue, ypred, metric_list[0], **kwargs)

        return self.get_metrics(ytrue, ypred, metric_list, **kwargs)

    def score_proba(self, ytrue, proba, metric=None, check_index=False, **kwargs):
        """
        Score based on predicted probabilities
        :param ytrue:       array, 0 or 1 valued, can have multiple columns.
        :param proba:       array, same shape as ytrue.
        :param metric:      str or list, metric(s) to use.
        Options:
            auc:            roc_auc_score
            aps:            average_precision_score
            roc_curve:      roc_curve
            pr_curve:       precision_recall_curve
        :param check_index: bool, check if indices match.
        :raise ValueError:  exceptions documented below
        :return:            metric(s)
        """
        if check_index:
            if not np.array_equal(ytrue.index, proba.index):
                raise ValueError("Indices of ytest and ypred don't match!")
        elif len(ytrue) != len(proba):
            raise ValueError("ytest and ypred don't have the same length!")

        if self.model_type != model_codes.CLASSIFICATION_MODEL_TYPE:
            raise ValueError("clf_score only works for classification problem!")

        metric_map = {'auc': 'roc_auc_score',
                      'aps': 'average_precision_score',
                      'roc_curve': 'roc_curve',
                      'pr_curve': 'precision_recall_curve'}

#         log_fun_info(logger)

        if isinstance(metric, str):
            metric = [metric]

        res = []
        for m in metric:
            if m not in metric_map:
                raise ValueError("Invalid Metric!")

            if len(metric) == 1:
                res.append(getattr(metrics_, metric_map[m])(ytrue, proba, **kwargs))
            elif m in kwargs:
                res.append(getattr(metrics_, metric_map[m])(ytrue, proba, **kwargs[m]))
            else:
                res.append(getattr(metrics_, metric_map[m])(ytrue, proba))

        if len(res) == 1:
            return res[0]
        else:
            return tuple(res)

    def cross_validation(self, xtrain, ytrain, metric, fold=None, n_jobs=-1, fit_params=None, check_index=False):
        """cross validation
        :param xtrain: DataFrame, training data.
        :param ytrain: Series, target column.
        :param metric: str, metric to score performance. Options include
            regression:
                'r2': 'r2_score'
                'meae': 'median_absolute_error'
                'mse': 'mean_squared_error'
                'mae': 'mean_absolute_error'
                'evs': 'explained_variance_score'
            classification:
                'f1': 'f1_score'
                'log_loss': 'log_loss'
                'precision': 'precision_score'
                'recall': 'recall_score'
                'accuracy': 'accuracy_score'
                'prfs': 'precision_recall_fscore_support'
                'confusion': 'confusion_matrix'
        :param fold: int, # of folds.
        :param n_jobs: int, n_jobs for parallelization
        :param fit_params: dict, parameters to fit model.
        :param check_index: bool, check if indices match.
        :raise ValueError:
        :return: list of performance scores for each fold.
        """
        if check_index:
            if not np.array_equal(xtrain.index, ytrain.index):
                raise ValueError("Indices of ytest and ypred don't match!")
        elif len(xtrain) != len(ytrain):
            raise ValueError("ytest and ypred don't have the same length!")

        return cross_val_score(self.model, xtrain, ytrain,
                               scoring=CROSS_VALIDATION_METRIC_MAP[metric],
                               cv=fold,
                               n_jobs=n_jobs,
                               fit_params=fit_params)

    def check_feature_importance(self, group=False):
        """
        Check feature importance
        :param group:       bool whether to group the categorical dummies.
        :param result_path: str file path to store the output.
        :raise ValueError:
        :return:            the feature importance table
        """
        if not hasattr(self, 'dummy_indep_cols'):
            raise ValueError("You haven't fit any model yet!")

        if not hasattr(self.model, 'feature_importances_'):
            raise ValueError("Model does not have attribute 'feature_importance_'!")

#         log_fun_info(logger)

        feature_importance = pd.DataFrame([self.model.feature_importances_, self.dummy_indep_cols]).T
        feature_importance.columns = ['score', 'feature']
        feature_importance['score'] = feature_importance['score'].astype('float')
        feature_importance = feature_importance.sort_values('score', ascending=False)

        if group:
            feature_importance['feature'] = feature_importance['feature'].map(lambda x: x.split('___')[0])
            feature_importance = feature_importance.groupby('feature')['score'].agg(['mean', 'count', 'sum'])
            feature_importance = feature_importance.sort_values('sum', ascending=False)
            feature_importance = feature_importance.reset_index()

        return feature_importance

    def get_feature_importances(self, residuals, xtrain, ytrain):
        """
        Feature Importance method used by Opal
        :param residuals:   array residuals (y - y^)
        :param xtrain:      int number of features
        :param ytrain:      int sample size
        :return:            pd.DataFrame feature importance table
        """
        feature_importances = None

        # Non-linear models:
        #   Accumulate feature importance through gradient descent while training
        if hasattr(self.model, 'feature_importances_'):
            feature_importances = model_helpers.feature_importance_table(
                self.model.feature_importances_,  # What guarantees these are in the same order?
                self.dummy_indep_cols,
                group=True
            )

        # Linear models:
        #   Try:  To calculate p-Values for each regressor
        #   Else: Use stepwise correlation based variable importance
        elif isinstance(self.model, LinearModel):
            # feature_importances = pd.DataFrame()
            coef_stats = OrderedDict((
                ('Feature', xtrain.columns),
                ('Co-Efficient', self.model.coef_),
            ))
            try:
                df = float(xtrain.shape[0] - xtrain.shape[1] - 1)
                sse = np.sum(residuals ** 2, axis=0) / df
                se = np.sqrt(np.diagonal(sse * np.linalg.inv(np.matrix(np.dot(xtrain.T, xtrain), dtype=np.float))))
                self.model.t_stats = self.model.coef_ / se
                # Two-sided test
                self.model.p_values = 2 * (
                    1 - stats.t.cdf(np.abs(self.model.t_stats), df)
                )
                coef_stats.update(OrderedDict((
                    ('t-Statistic', self.model.t_stats),
                    ('p-Value', self.model.p_values)
                )))
            except np.linalg.linalg.LinAlgError:
                f_scores, p_values = sklearn.feature_selection.f_regression(
                    X=xtrain[self.dummy_indep_cols],
                    y=ytrain,
                    center=True
                )
                self.model.f_values = f_scores
                self.model.p_values = p_values
                coef_stats.update(OrderedDict((
                    ('F-Score', self.model.f_values),
                    ('p-Value', self.model.p_values)
                )))

            table_data = pd.DataFrame(coef_stats)
            return table_data.sort_values('p-Value', ascending=True)

        if feature_importances is not None:
            _columns = ['feature', 'dummies', 'percent_contribution']
            subset = feature_importances[_columns]
            feature_importances = subset.rename(
                columns={
                    'feature': 'Feature',
                    'dummies': 'Number of Dummies',
                    'percent_contribution': 'Percent Contribution'
                }
            )

        return feature_importances

    def fit_predict(self, xtrain, ytrain, xtest, indep_cols,
                    sample_weights=None,
                    groupby=None,
                    return_proba=False,
                    pos_label=None,
                    min_proba_for_pos=None,
                    check_index=True):
        """
        Single model single test
        :param xtrain:              DataFrame, xtrain
        :param ytrain:              Series, ytrain
        :param xtest:               DataFrame, xtest
        :param indep_cols:          list, the set of column names to build the model.
        :param sample_weights:      Series, column to use as sample weight to fit the model.
        :param groupby:             Series, column to group by. Contains indexes for both train and test.
        :param min_proba_for_pos:   float, probability cut-off for positive class.
                                    Only applies for two-class problems.
        :param return_proba:        bool, whether to write predicted probability into the output file.
        :param pos_label:           str, positive label.
        :param check_index:         bool, check if xtrain, ytrain, sample_weights match, and if xtest and ytest match.
        :raise:                     ValueError
        :return:                    model performance, specified by 'return_type'.
        """

#         log_fun_info(logger)

        if groupby is not None:
            groupby_train = groupby.ix[xtrain.index]
            groupby_test = groupby.ix[xtest.index]
            pred, proba = pd.DataFrame(), pd.DataFrame()
            split_vals = groupby.unique()
            for val in split_vals:
                xtrain_subset = xtrain[groupby_train == val]
                xtest_subset = xtest[groupby_test == val]
                ytrain_subset = ytrain[groupby_train == val]
                if sample_weights is not None:
                    sample_weights_subset = sample_weights[groupby_train == val]
                else:
                    sample_weights_subset = None
                self.fit_model(xtrain_subset, ytrain_subset, indep_cols, sample_weights_subset, check_index)

                if return_proba:
                    pred_sub, proba_sub = self.predict(xtest_subset, True, pos_label, min_proba_for_pos)
                    pred, proba = pd.concat([pred, pred_sub], axis=1), pd.concat([proba, proba_sub], axis=1)
                else:
                    pred_sub = self.predict(xtest_subset, False, pos_label, min_proba_for_pos)
                    pred = pd.concat([pred, pred_sub], axis=1)

            pred = pred.reindex(xtest.index)
            if return_proba:
                proba = proba.reindex(xtest.index)
                proba.fillna(0, inplace=True)
                return pred, proba
            return pred
        else:
            self.fit_model(xtrain, ytrain, indep_cols, sample_weights, check_index)
            return self.predict(xtest, return_proba, pos_label, min_proba_for_pos)

    def model_comparison(self, xtrain, ytrain, xtest, ytest, indep_cols,
                         model_list='all',
                         sample_weights=None,
                         groupby=None,
                         pos_label=None,
                         min_proba_for_pos=None,
                         metric=None,
                         check_index=False,
                         **metric_params):
        """Multiple model comparisons using single test.
        :param xtrain: DataFrame, xtrain
        :param ytrain: Series, ytrain
        :param xtest: DataFrame, xtest
        :param ytest: Series, ytest
        :param indep_cols: list, the set of column names to build the model.
        :param model_list: list, the set of model names to apply.
        :param sample_weights: Series, column to use as sample weight to fit the model.
        :param min_proba_for_pos: float, probability cut-off for positive class. Only applies for two-
                                  class problems.
        :param metric: str, metric to score performance. Options include
            regression:
                'r2': 'r2_score'
                'meae': 'median_absolute_error'
                'mse': 'mean_squared_error'
                'mae': 'mean_absolute_error'
                'evs': 'explained_variance_score'
            classification:
                'f1': 'f1_score'
                'log_loss': 'log_loss'
                'precision': 'precision_score'
                'recall': 'recall_score'
                'accuracy': 'accuracy_score'
                'prfs': 'precision_recall_fscore_support'
                'confusion': 'confusion_matrix'
        :param metric_params: dict, additional parameters for the metric method.
        :param groupby: column name, fit the model group by the column.
        :param pos_label: str, positive label.
        :param check_index: bool, check if xtrain, ytrain, sample_weights match, and if xtest and
            ytest match.
        :raise ValueError:
        :return: str, model settings and F1 scores.
        """
        if self.models is None:
            raise ValueError("Models are not defined!")

#         log_fun_info(logger)

        if model_list == 'all':
            model_list = self.models.keys()

        res = {}
        for model_name in model_list:
            self.model = self.models[model_name]

            pred = self.fit_predict(xtrain, ytrain, xtest, indep_cols, sample_weights, groupby,
                                    False, pos_label, min_proba_for_pos, check_index)

            res[model_name] = self.eval_model(ytest, pred, metric, check_index, **metric_params)
        return res

    def dim_reduction(self, mld, exclude_cols=[], n_components=2, model='pca', inplace=True):
        """Dimension reduction using PCA or ICA.
        :param mld: MachineLearningData object. Note that MLD should contain dummy data.
        :param exclude_cols: list, the set of column names to exclude from dimension reduction. Note
                            that the exclude set would still be included in the resulting data.
        :param n_components: int, the number of components resulting from dimension reduction.
        :param model: 'pca' or 'ica'.
        :param inplace: bool, whether to alter the data inplace or not.
        :raise ValueError:
        :return: if inplace == False, return the data frame; else None.
        """
#         log_fun_info(logger)

        if model == 'pca':
            drm = PCA(n_components)
        elif model == 'ica':
            drm = FastICA(n_components)
        else:
            raise ValueError("Model input is invalid!")
        indep_cols = mld.data.columns.difference(exclude_cols)
        temp = pd.DataFrame(drm.fit_transform(mld.data[indep_cols]), index=mld.data.index,
                            columns=[model + str(x) for x in range(n_components)])
        if inplace:
            mld.data = pd.concat([mld.data[exclude_cols], temp], axis=1)
        else:
            return pd.concat([mld.data[exclude_cols], temp], axis=1)

        # TODO:
        # mlmodel.decision_tree_plot needs to be analyzed; it produces binary of .png not file
        # Description is no longer accurate

    def decision_tree_plot(self, format='svg', return_dot=False):
        """Generate image file or binary with specificed format for decision tree plot.
        :param format: str, svg, png, jpg, etc
        :param return_dot: bool, return dot file or not
        :return: image_binary
        """
        if not hasattr(self, 'dummy_indep_cols'):
            self.__raise_value_error("You haven't fit any model yet!")

        def change_x(match):
            """inline function
            :param match: the match object from re.
            :return: r
            """
            return self.__change_x(match, self.dummy_indep_cols)

        tree_dot = tree.export_graphviz(self.model, out_file=None, special_characters=True)

        # write_obj_to_file('test', 'graphviz_raw.log', tree_dot)  # DELETE AFTER TEST
        # Cut value from label in intermediate nodes
        tree_dot = re.sub(r'(\[label=<X<SUB>[\d]+<\/SUB>.*)<br\/>value = \[[^\]]*\]([^\]])*', r'\1\2', tree_dot)
        # Add formatting to lables in intermediate nodes
        tree_dot = re.sub(
            r'(label=<X<SUB>[\d]+<\/SUB>[^\]]*samples = [\d]+>)',
            r'\1, style="rounded, filled", color="#eaeaea"',
            tree_dot
        )

        if self.model_type == model_codes.CLASSIFICATION_MODEL_TYPE:
            # Change leaf name to match index from model classes
            tree_dot = re.sub(r'(\[label[^\]]*)value = \[\s*(.*?)\s*\]([^\]]*)', self.__change_tree_leaf, tree_dot)
            # Same as bavoe but for case of leaf name change where value is not array in brackets
            tree_dot = re.sub(r'(\[label[^\]]*)value = \s*(.*?)\s*([^\]]*)', self.__change_tree_leaf, tree_dot)
        else:
            tree_dot = re.sub('value', 'average', tree_dot)

        # Adds coloration for yes / no path
        tree_dot = re.sub(r"([\d]+)\s*->\s*([\d]+).*;", self.__change_a_tree_arrow, tree_dot)
        # Translate <= to Not Equal; > to Equal; changed to %le; and %gt;
        tree_dot = re.sub(r'X<SUB>(\d+)<\/SUB>( (\&le\;|\&gt\;) [\d.-]+)', change_x, tree_dot)
        # Escape any html unsafe characters from change_x substitution

        if return_dot:
            return tree_dot

        # write_obj_to_file('test', 'graphviz_formatted.log', tree_dot)  # DELETE AFTER TEST
        env = {
            "PATH": os.pathsep.join([
                GRAPHVIZ_UNIX_DIR,
                os.environ["PATH"]
            ])
        }

        src = MLSource(tree_dot, format=format)
        image_binary = src.pipe(env=env)
        return image_binary

    def __change_tree_leaf(self, match):
        """Helper function for decision_tree_plot. Change the display of leaf nodes.
        :param match: the match object from re.
        :return: modified dot string.
        """
        sample_ct = re.split('\D+', match.group(2).strip().replace('.', ''))
        s = match.group(1)
        for i in range(len(sample_ct)):
            if i % 2 == 0 and i > 0:
                s += '<br/>'
            s += ' ' + str(self.model.classes_[i]) + ' = ' + str(sample_ct[i]) + ' '
        return s + match.group(3) + ', style="rounded, filled", color="#b3e0ff"'

    def __change_a_tree_arrow(self, match):
        """Helper function for decision_tree_plot. Change the display of arrows.
        :param match: the match object from re.
        :return: modified dot string.
        """
        if (int(match.group(1)) + 1 == int(match.group(2))):
            result = match.group(1) + " -> " + match.group(
                2) + ' [label=No, color="#FF0000", fontcolor="#FF0000"] ;'
        else:
            result = match.group(1) + " -> " + match.group(2) + ' [label=Yes, color=green, fontcolor="#00cc00"] ;'
        return result

    def __change_x(self, match, columns):
        """Helper function for decision_tree_plot.
        :param match: the match object from re.
        :param columns: list, the set of dummy column names used to build the tree.
        :return: modified variable names.
        """
        i = int(match.group(1))  # id
        # op = match.group(3)  # < or >
        s = match.group(2)
        if columns[i].find('___') != -1:
            items = columns[i].split('___')
            #             if op == '<' or op == '<=':
            #                 return items[0] + ' != ' + items[1]
            #             else:
            return ' = '.join(map(html.escape, (items[0], items[1])))
        else:
            s = s.replace("&le;", "&gt;")  # TODO: Wait what? Why are inverting the logic?
            return columns[i] + s

    def build_leaf_path(self, dtypes):
        """For decision tree only.
        Traverse the tree in DFS fashion for each leaf node and return a data frame, with each
        row being the path of a leaf and each column corresponding a feature. The order of the path
        is ignored. Multiple encounters of the same feature in one path are combined.
        :param dtypes: dict, data types.
        :raise ValueError:
        :return: data frame containing the paths.
        """
        if not hasattr(self, 'dummy_indep_cols'):
            raise ValueError("You haven't fit any model yet!")

#         log_fun_info(logger)
        empty_path = self.__init_empty_path(dtypes)
        res = []
        self.__recur_path(0, empty_path, res)
        if self.model_type == model_codes.CLASSIFICATION_MODEL_TYPE:
            result = pd.DataFrame(res, columns=list(self.model.classes_) + ['Purity', 'Purity*Size', 'Factors'])
            result.sort_values('Purity*Size', ascending=False, inplace=True)
        else:
            result = pd.DataFrame(res, columns=['Average', 'Size', self.model.criterion.capitalize(), 'Factors'])
            result.sort_values('Size', ascending=True, inplace=True)

        # result.dropna(axis=1, how='all', inplace=True)
        return result

    def __recur_path(self, node_id, path, res):
        """Helper function for build_leaf_path. recursively run for each node.
        :param node_id: node id in sklearn decision tree tree_.
        :param path: path to current node.
        :param res: list to store the result.
        """
        right_node = self.model.tree_.children_right[node_id]
        left_node = self.model.tree_.children_left[node_id]
        columns = self.dummy_indep_cols
        if right_node == -1:
            if self.model_type == model_codes.CLASSIFICATION_MODEL_TYPE:
                values = self.model.tree_.value[node_id][0]  # sample_ct for classification
            else:
                # average and n_sample for regression
                values = list(self.model.tree_.value[node_id][0]) + [self.model.tree_.n_node_samples[node_id]]
            impurity = self.model.tree_.impurity[node_id]
            self.__save_path(values, impurity, path, res)
        else:
            dummy_id = self.model.tree_.feature[node_id]
            dummy = columns[dummy_id]

            right_path = copy.deepcopy(path)
            left_path = copy.deepcopy(path)

            if dummy.find("___") != -1:
                column, category = dummy.split('___')
                right_path[column]["="] = category
                left_path[column]["!in"].append(category)
            else:
                threshold = self.model.tree_.threshold[node_id]
                if path[dummy][">"] is None or path[dummy][">"] < threshold:
                    right_path[dummy][">"] = threshold
                if path[dummy]["<="] is None or path[dummy]["<="] > threshold:
                    left_path[dummy]["<="] = threshold
            self.__recur_path(right_node, right_path, res)
            self.__recur_path(left_node, left_path, res)

    def __init_empty_path(self, dtypes):
        """Construct empty path.
        :param dtypes: dict, the data types of independent columns.
        :return: an empth path.
        """
        empty_path = {}
        for column in self.indep_cols:
            if dtypes[column] == 'category' or dtypes[column] == 'object':
                empty_path[column] = {"=": None, "!in": []}
            else:
                empty_path[column] = {"<=": None, ">": None}
        return empty_path

    def __save_path(self, values, impurity, path, res):
        """Save path of a leaf node to the list.
        :param values: sample_ct for classification, average and n_samples for regression
        :param impurity: impurity score.
        :param path: the path to the leaf node.
        :param res: list to store the result.
        """
        row = list(values)

        if self.model_type == model_codes.CLASSIFICATION_MODEL_TYPE:
            row.append(1 - impurity)
            row.append((1 - impurity) * sum(values))
        else:
            row.append(impurity)

        tokens = ""
        for column in self.indep_cols:
            if ">" in path[column]:
                if path[column][">"] is not None and path[column]["<="] is not None:
                    n1 = round(path[column][">"], 4)
                    n2 = round(path[column]["<="], 4)
                    tokens += "%s < %s <= %s; " % (n1, column, n2)
                elif path[column]["<="] is not None:
                    n2 = round(path[column]["<="], 4)
                    tokens += "%s <= %s; " % (column, n2)
                elif path[column][">"] is not None:
                    n1 = round(path[column][">"], 4)
                    tokens += "%s > %s; " % (column, n1)
            else:
                if path[column]["="] is not None:
                    tokens += "{} = {}; ".format(column, path[column]["="])
                elif len(path[column]["!in"]) > 0:
                    if len(path[column]["!in"]) == 1:
                        tokens += "{} != {}; ".format(column, path[column]["!in"][0])
                    else:
                        tokens += "{} not in {}; ".format(column, path[column]["!in"]).replace("'", "")
        if len(tokens) > 0:
            tokens = tokens[:-2]  # remove the last "; "
        row.append(tokens)
        res.append(row)
