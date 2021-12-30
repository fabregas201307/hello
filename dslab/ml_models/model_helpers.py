
import pandas as pd


def feature_importance_table(feature_importances, dummy_indep_columns, group=False):
    """
    Transform feature importance statistics; optionally grouping dummy columns by categorical feature input
    :param dummy_indep_columns:    list of all dummified columns used to train model
    :param feature_importances:    np.Array feature importance statistics by column (feature)
    :param group:                  bool group or no
    :return:                       pd.DataFrame (feature, num dummies, percent sum F-score)
    """
    table_data = pd.DataFrame([feature_importances, dummy_indep_columns]).T
    table_data.columns = ['score', 'feature']
    table_data['score'] = table_data['score'].astype('float')
    table_data = table_data.sort_values('score', ascending=False)

    if group:
        # Split into original categorical groups by column name pre training
        table_data['feature'] = table_data['feature'].map(lambda x: x.split('___')[0])
        table_data = table_data.groupby('feature')['score'].agg(['mean', 'count', 'sum'])
        table_data = table_data.sort_values('sum', ascending=False)
        # TODO: Confirm this math works for linear models
        table_data['sum'] = table_data['sum'] / table_data['sum'].sum()
        table_data['sum'] = table_data['sum'] * 100
        table_data = table_data.reset_index()
        table_data.rename(
            columns={
                'feature': 'feature',
                'count': 'dummies',
                'sum': 'percent_contribution'
            },
            inplace=True
        )

    return table_data


    


