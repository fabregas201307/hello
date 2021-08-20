from flask import Flask
from flask import request
from flask import jsonify

app = Flask(__name__)

@app.route("/add_index", method=["POST"])
def add_index():
    """
    user upload data index, write to datalake (or Aries write-back)
    user input: (1) index_config [index_id, aries_id, vendor, periodicity, index_definition]
                (2) index [index_id, valid_from, valid_to, period_from, period_to, value]
    """
    json_dict = request.get_json()
    return "Welcome to systematic evaluation, add_index."

@app.route("/search_index", method=["GET"])
def search_index():
    """
    user search data index by id
    """
    json_dict = request.get_json()
    return "Welcome to systematic evaluation, search_index."

@app.route("/build_feature", method=["POST"])
def build_feature():
    """
    user input: feature_config [feature_id, backtest_type, backtest_criteria, index_id,
                                calendar_id, transformation(YoY, QoQ, etc..)]
    This computing will take time, so send message to message queue, and another worker will consume
    from the queue to do the real work. Auto-scale will be on the worker component if message pile up.
    operationalized feature generations from alternative data, for adhoc new data,
    you will need your own process to generate features, then potentially integrate to server
    """
    json_dict = request.get_json()

    # worker: run aligner step, should take less than 15 minutes per feature_id
    # worker: dump features to S3 (feature library)
    # worker: return jsonify of S3 path ?

    return jsonify(s3_path)

@app.route("/search_feature", method=["GET"])
def search_feature():
    """
    user input: feature_id
    return: feature_config and feature
    """
    json_dict = request.get_json()


@app.route("/run_back_test", method=["POST"])
def run_back_test():
    """
    user input: score_config [score_id, score_metric, feature_id, feature_role,
                                backtest_date, backtest_from, backtest_to]
                one message should contain all about one score_id, because some feature_id is actually target variable to predict,
                and some are part of multi-variate predictor, you can't just input part of one score_id, must be all rows.
    This computing run faster compared with build_feature, but still send message to message queue, and another worker will consume
    from the queue to run the calculator. Auto-scale will be on the worker component if message pile up.
    store:
        (1) input score_config;
        (2) calculated score;
    """
    json_dict = request.get_json()


@app.route("/search_back_test", method=["GET"])
def search_back_test():
    """
    user input: score_id
    return: score_config and score
    """
    json_dict = request.get_json()



