import os
import numpy as np
import pandas as pd
import xgboost
from sklearn.ensemble.forest import RandomForestRegressor
from ..service.prediction_service import do_classify, get_features_importance
from ..model.config_manager import Config_manager
from ..helper.file_helper import get_file_path
cm = Config_manager()

fileDir = os.path.dirname(os.path.abspath(__file__))


class Prediction(object):

    def __init__(self, datastore):
        self.datastore = datastore
        self.name = datastore.name

    def _create_X_Y_per_site(self, site_id, label):
        self.training_predictors = self.datastore.get_training_set(site_id)

        self.forecast_predictors = self.datastore.get_forecasts_set(site_id)
        print "training..."
        self.X_training, self.y, self.training_date = self._to_X_y(
            self.training_predictors, label)
        print "test :"
        self.X_test, _, self.prediction_date = self._to_X_y(
            self.forecast_predictors)

    def _get_features(self):
        features = {k: v for k, v in cm.features.iteritems() if cm.features[
            k].is_predictor}
        features = self._filter_on_regularized(features)

        # if period id D (day), both training and forecasts have
        # 'mintemperature' and 'maxtemperature'
        if self.datastore.period == 'D':
            features.remove('temperature_reg')

        # if period is H (hour), there is no min/max values, only
        # 'temperature' for the training set.
        if self.datastore.period == 'H':
            features.remove('mintemperature_reg')
            features.remove('maxtemperature_reg')

        return features

    def _filter_on_regularized(self, features):
        regularize = [k for k, v in features.iteritems() if v.regularize]

        features = [
            feature for feature in features if feature not in regularize]
        regularized = [feature + "_reg" for feature in regularize]
        return features + regularized

    def _to_X_y(self, data, label=None):

        features = self._get_features()

        if label:
            return data[features].values, data[label].values, data['date'].values
        else:
            return data[features].values, 0, data['date'].values

    def make_prediction(self, site_id, label):
        self._create_X_Y_per_site(site_id, label)

        self.xgbooster(label)

        clf_RDM = {'params': {'n_estimators': [300], 'bootstrap': [
            True], 'criterion': ['mse']}, 'clf': RandomForestRegressor()}

        clf = clf_RDM['clf']
        params = clf_RDM['params']

        clf_rdm, Xtrain, ytrain, Xtest, ytest, r2 = \
            do_classify(clf, params, self.X_training, self.y)

        prediction = clf_rdm.predict(self.X_test)
        self.r2 = r2

        self.forecast_predictors[label] = pd.Series(
            prediction, index=self.forecast_predictors.index)

        self.features_weighted = get_features_importance(
            clf_rdm, self._get_features())

        print self.features_weighted

        self.forecast_predictors.to_csv(get_file_path("data/store/" +
                                                      self.name +
                                                      "_predictions_" +
                                                      self.datastore.period +
                                                      "_" +
                                                      label + ".csv",
                                                      fileDir), sep=";")

    def RMSE(self, df, predictor):
        predicted = predictor
        observed = 'observed'
        df['error'] = df[observed] - df[predicted]
        df.error = np.square(df.error)
        total_error = np.sum(np.sqrt(df.error))
        print df.error
        rmse = np.mean(np.sqrt(df.error))
        print "rmse", rmse
        accuracy = 100 - total_error / df[predicted].sum() * 100
        print "accuracy", accuracy
        return rmse, accuracy

    def xgbooster(self, label):
        model = xgboost.XGBRegressor()
        model.fit(self.X_training, self.y)
        print model
        predictions = model.predict(self.X_test)
        print type(predictions)

        self.forecast_predictors[label + "_xgboost"] = pd.Series(
            predictions, index=self.forecast_predictors.index)
