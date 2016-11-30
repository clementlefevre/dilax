import logging
from ..model.config_manager import Config_manager

config_manager = Config_manager()


def regularize_training(df):
    for col in df.columns.tolist():
        if config_manager.features[col].regularize:
            df[col + "_reg"] = df.groupby('idbldsite')[col].apply(
                lambda x: (x - x.mean()) / x.std())
            logging.info("{0} :  has been regularized for training set".format(
                         col))
    df = df.fillna(0)

    return df


def _set_regularization_params(df):
    column_to_regularize = [col for col in df.columns.tolist(
    ) if col in config_manager.features and config_manager.features[col].regularize]
    print "********************************"
    print column_to_regularize

    training_set_grouped = df.groupby('idbldsite')[column_to_regularize]
    x_mean = training_set_grouped.mean()
    x_std = training_set_grouped.std()
    return {'x_mean': x_mean, 'x_std': x_std}


def regularize_forecasts(df, regularization_params):
    x_mean = regularization_params['x_mean']
    x_std = regularization_params['x_std']

    for col in df.columns.tolist():

        def standardize(row):
            idbldsite = row.idbldsite

            if idbldsite in x_mean.index and idbldsite in x_std.index:
                if x_std.loc[idbldsite][col] != 0:
                    standardized = (row[col] - x_mean.loc[idbldsite][col]
                                    ) / x_std.ix[idbldsite][col]
                else:
                    standardized = 0
            else:
                standardized = 0

            return standardized

        if config_manager.features[col].regularize:
            df[col + "_reg"] = df.apply(
                standardize, axis=1)
            logging.info("{0} :  has been regularized for forecast set".format(
                         col))
    df = df.fillna(0)
    return df
