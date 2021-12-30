
from sklearn import svm
from sklearn import tree
from sklearn import ensemble
from sklearn import neighbors
from sklearn import naive_bayes
from sklearn import linear_model
from sklearn import discriminant_analysis


REGRESSION_MODEL_TYPE = 'regression'
CLASSIFICATION_MODEL_TYPE = 'classification'

MODEL_TYPES = list({REGRESSION_MODEL_TYPE, CLASSIFICATION_MODEL_TYPE})


"""
Regression
"""


RUN_ALL_REGRESSORS = 'run_all_regressors'

# Regression model codes

RIDGE = 'ridge'
LASSO = 'lasso'
ELASTIC_NET = 'enet'
BAGGING_REGRESSOR = 'bag'
ADA_BOOST_REGRESSOR = 'ab'
RANSAC_REGRESSOR = 'ransac'
EXTRA_TREES_REGRESSOR = 'ert'
ORDINARY_LEAST_SQUARES = 'ols'
KNEIGHBORS_REGRESSOR = 'knnrgr'
BAYES_RIDGE_REGRESSOR = 'bayes'
DECISION_TREE_REGRESSOR = 'dtrgr'
RANDOM_FOREST_REGRESSOR = 'rfrgr'
GRADIENT_BOOSTING_REGRESSOR = 'gbrgr'
SCALAR_VECTOR_MACHINE_REGRESSOR = 'svr'

# Regression model constructors

REGRESSION_MODELS = {
    # Linear models
    RIDGE: linear_model.Ridge,
    LASSO: linear_model.Lasso,
    ELASTIC_NET: linear_model.ElasticNet,
    BAYES_RIDGE_REGRESSOR: linear_model.BayesianRidge,
    RANSAC_REGRESSOR: linear_model.RANSACRegressor,
    ORDINARY_LEAST_SQUARES: linear_model.LinearRegression,
    # Ensemble
    ADA_BOOST_REGRESSOR: ensemble.AdaBoostRegressor,
    GRADIENT_BOOSTING_REGRESSOR: ensemble.GradientBoostingRegressor,
    RANDOM_FOREST_REGRESSOR: ensemble.RandomForestRegressor,
    EXTRA_TREES_REGRESSOR: ensemble.ExtraTreesRegressor,
    BAGGING_REGRESSOR: ensemble.BaggingRegressor,
    # Other
    SCALAR_VECTOR_MACHINE_REGRESSOR: svm.SVR,
    KNEIGHBORS_REGRESSOR: neighbors.KNeighborsRegressor,
    DECISION_TREE_REGRESSOR: tree.DecisionTreeRegressor,
}


"""
Classification
"""


RUN_ALL_CLASSIFIERS = 'run_all_classifiers'

# Classification model codes

BAGGING = 'bag'
LOGISTIC_REGRESSION = 'lg'
ADA_BOOST_CLASSIFIER = 'ab'
GUASSIAN_NAIVE_BAYES = 'gnb'
BERNOULI_NAIVE_BAYES = 'bnb'
KNEIGHBORS_CLASSIFIER = 'knn'
EXTRA_TREES_CLASSIFIER = 'ert'
SUPPORT_VECTOR_MACHINE = 'svm'
DECISION_TREE_CLASSIFIER = 'dt'
RANDOM_FOREST_CLASSIFIER = 'rf'
MULTINOMIAL_NAIVE_BAYES = 'mnb'
GRADIENT_BOOSTING_CLASSIFIER = 'gb'
LINEAR_DISCRIMINANT_ANALYSIS = 'lda'
LINEAR_SCALAR_VECTOR_MACHINE = 'lsvc'
QUADRATIC_DISCRIMINANT_ANALYSIS = 'qda'

# Classification model constructors

CLASSIFICATION_MODELS = {
    # Ensemble
    BAGGING: ensemble.BaggingClassifier,
    ADA_BOOST_CLASSIFIER: ensemble.AdaBoostClassifier,
    EXTRA_TREES_CLASSIFIER: ensemble.ExtraTreesClassifier,
    RANDOM_FOREST_CLASSIFIER: ensemble.RandomForestClassifier,
    GRADIENT_BOOSTING_CLASSIFIER: ensemble.GradientBoostingClassifier,
    # Discriminant Analysis
    LINEAR_DISCRIMINANT_ANALYSIS: discriminant_analysis.LinearDiscriminantAnalysis,
    QUADRATIC_DISCRIMINANT_ANALYSIS: discriminant_analysis.QuadraticDiscriminantAnalysis,
    # Naive bayes
    GUASSIAN_NAIVE_BAYES: naive_bayes.GaussianNB,
    BERNOULI_NAIVE_BAYES: naive_bayes.BernoulliNB,
    MULTINOMIAL_NAIVE_BAYES: naive_bayes.MultinomialNB,
    KNEIGHBORS_CLASSIFIER: neighbors.KNeighborsClassifier,
    # Other
    SUPPORT_VECTOR_MACHINE: svm.SVC,
    LINEAR_SCALAR_VECTOR_MACHINE: svm.LinearSVC,
    LOGISTIC_REGRESSION: linear_model.LogisticRegression,
    DECISION_TREE_CLASSIFIER: tree.DecisionTreeClassifier,
}


"""
Unified
"""


ML_MODELS_BY_TYPE = {
    REGRESSION_MODEL_TYPE: REGRESSION_MODELS,
    CLASSIFICATION_MODEL_TYPE: CLASSIFICATION_MODELS
}

# NOTE: Not possible becuase of overlapping model codes across types
# ML_MODELS = merge_dicts(REGRESSION_MODELS, CLASSIFICATION_MODELS)
