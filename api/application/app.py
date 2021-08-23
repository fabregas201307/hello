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
    index_config, index = json_dict["index_config"], json_dict["index"]
    index_id = index_config["index_id"]
    if index.shape[0] > 0:
        s3_path = f"systematic_eval/index/{today}/{index_id}.csv"
        upload_dataframe(index, s3_path)
    if index_config.shape[0] > 0:
        s3_path = f"systematic_eval/index_config/{today}/{index_id}.csv"
        upload_dataframe(index_config, s3_path)
    return "Welcome to systematic evaluation, add_index."

@app.route("/search_index", method=["GET"])
def search_index():
    """
    user search data index by index_id,  it could be a list
    """
    json_dict = request.get_json()
    index_ids = json_dict["index_id"]
    q_index = """
    select *
    from sda_workspace.systematic_eval_tmp_index
    where index_id in '{}'
    """.format(index_ids)
    index_df = query(q_index)

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
    feature_config = json_dict["feature_config"]

    # need to upload feature_config to S3 or redshift


    # end of uploading feature_config

    index_id, calendar_id = feature_config["index_id"], feature_config["calendar_id"]

    q_index = """
    select *
    from sda_workspace.systematic_eval_tmp_index
    where index_id = '{}'
    """.format(index_id)

    index_single = query(q_index)

    q_calendar = """
    select *
    from sda_workspace.systematic_eval_tmp_calendar
    where calendar_id = '{}'
    """.format(calendar_id)
    calendar_single = query(q_calendar)

    from sectordata.aligner import Aligner
    aligner = Aligner(data=index_single.copy(), calendar=calendar_single.copy(), backtest_Vintage=feature_config["backtest_criteria"])
    feature = aligner.generate_feature(transformation=feature_config["transformation"], method="percentage").rename(columns={"feature": "value"})
    feature["feature_id"] = feature_config["feature_id"]
    feature = feature[["feature_id", "period_from", "period_to", "value"]]
    feature = feature.loc[feature["value"].notnull()]

    if feature.shape[0] > 0:
        s3_path = f"systematic_eval/feature/{today}/{feature_id}.csv"
        upload_dataframe(feature, s3_path)
    # worker: run aligner step, should take less than 15 minutes per feature_id
    # worker: dump features to S3 (feature library)
    # worker: return jsonify of S3 path ?

    return jsonify(s3_path)

@app.route("/search_feature", method=["GET"])
def search_feature():
    """
    user input: feature_id
    return: feature_config and feature
    feature library is in S3, not redshift?
    """
    json_dict = request.get_json()
    feature_ids = json_dict["feature_id"]
    q_index = """
    select *
    from sda_workspace.systematic_eval_tmp_feature
    where feature_id in '{}'
    """.format(feature_ids)
    feature_df = query(q_index)

    return "Welcome to systematic evaluation, search_feature."


@app.route("/run_back_test", method=["POST"])
def run_back_test():
    """
    user input: score_config [score_id, score_metric, feature_id, feature_role,
                                backtest_date, backtest_from, backtest_to]
                one message should contain all about one score_id, because some feature_id is actually target variable to predict,
                and some are part of multi-variate predictor, you can't just input part of one score_id, must be all rows.
    This computing run faster compared with build_feature, but still send message to message queue, and another worker will consume
    from the queue to run the calculator. Auto-scale will be on the worker component if message pile up.
    store in both S3 and redshift(overwrite):
        (1) input score_config;
        (2) calculated score;
    """
    import pandas as pd
    json_dict = request.get_json()
    score_config = json_dict["score_config"]  # could be multiple rows, assuming only one score_id
    feature_id_list, feature_role_list = score_config["feature_id"], score_config["feature_role"] # must be 1-1 match, usually list size 2, 1 predictor, 1 target
    feature_list = list()
    score_metric, backtest_from, backtest_to = score_config["score_metric"], score_config["backtest_from"], score_config["backtest_to"]
    # joined = pd.DataFrame()
    for feature_id in feature_id_list:
        q_feature = """
        select *
        from sda_workspace.systematic_eval_tmp_feature
        where feature_id = '{}'
        """.format(feature_id)
        cur_feature = query(q_feature)
        feature_list.append(cur_feature)

    feature1, feature2 = feature_list[0], feature_list[1]
    scores = list()

    if feature1.shape[0] > 0 and feature2.shape[0] > 0:
        if score_metric == "correlation":
            score_metric = "pearson"

        joined = feature1.merge(feature2, on=["period_from", "period_to"])
        joined.columns = ["period_from", "period_to", "data_value", "kpi_value"]
        joined["as_of_date"], joined["sid"] = pd.to_datetime("1900-01-01"), "dummy"

        # make sure only full periods are captured based on backtest instruction
        # i.e. period_from~to should be equal to or narrower than backtest range
        joined = joined.loc[(backtest_from <= joined["period_from"]) & (joined["period_to"] <= backtest_to)]
        joined = joined.sort_values("period_to")
        joined["diff"] = joined["period_to"].diff()
        joined["diff"] = joined["diff"].apply(lambda x: x.days)

        # the gap between each quarter should be less than 120 days
        # minimum 8 data points needed for the score calculation
        checks = (joined["diff"].max() < 120) and (len(joined) >= 8)
        joined = joined.drop(["diff"], axis=1)

        if checks:
            calculator = Calculator(data=joined.copy())
            vintage_date = pd.to_datetime("1900-01-01")
            score = calculator.calculate_stats_data_kpi(stats_method=score_metric, vintage_date=vintage_date)[vintage_date]["dummy"]
            if score == score: # not null
                scores.append([score_id, score, joined["period_from"].min(), joined["period_to"].min(),
                               joined["period_from"].max(), joined["period_to"].max()
                                ])

    scores_df = pd.DataFrame(scores, columns=["score_id", "value", "min_period_from", "min_period_to", "max_period_from", "max_period_to"])

    upload_dataframe(scores_df, f"systematic_eval/score/{today}/score.csv")

    # create redshift table from uploaded score csv files, do we really need to drop table, not append to it?
    q_scores_upload = f"""
    drop table if exists sda_workspace.systematic_eval_tmp_score;

    create table sda_workspace.systematic_eval_tmp_score (
        score_id varchar,
        value float,
        min_period_from date,
        min_period_to date,
        max_period_from date,
        max_period_to date
    );

    grant select on sda_workspace.systematic_eval_tmp_score to pmd_services_ro;
    grant select on sda_workspace.systematic_eval_tmp_score to svc_sda;

    COPY sda_workspace.systematic_eval_tmp_score
    FROM 's3://bam-s3-ue1-p-sda-ref-data/systematic_eval/score/{today}'
    iam_role 'arn:aws:iam::523863455948:role/bam-dataprod-role-redshift_role_assumption,arn:aws:iam::523863455948:role/bam-dataprod-role-redshift_sda'
    region 'us-east-1'
    delimiter ','
    escape ACCEPTINVCHARS TRUNCATECOLUMNS ignoreheader 1 EMPTYASNULL;
    """
    execute_query(q_scores_upload)

@app.route("/search_back_test", method=["GET"])
def search_back_test():
    """
    user input: score_id
    return: score_config and score
    """
    json_dict = request.get_json()
    score_ids = json_dict["score_id"]
    q_index = """
    select *
    from sda_workspace.systematic_eval_tmp_score
    where score_id in '{}'
    """.format(score_ids)
    score_df = query(q_index)
    return "Welcome to systematic evaluation, search_back_test."
