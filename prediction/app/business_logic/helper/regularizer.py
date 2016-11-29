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


def regularize_forecasts(df):
    pass
