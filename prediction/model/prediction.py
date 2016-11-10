
from sklearn.ensemble.forest import RandomForestRegressor
from service.prediction_service import do_classify, cv_optimize


import pandas as pd
from model.config_manager import Config_manager
cm = Config_manager()


class Prediction(object):

    def __init__(self, datastore):
        self.datastore = datastore
        self.name = datastore.name

    def _create_X_Y_per_site(self, site_id, label):
        self.training_predictors = self.datastore.get_training_set(site_id)

        self.forecast_predictors = self.datastore.get_forecasts_set(site_id)
        print "training :"
        self.X_training, self.y, self.training_date = self._to_X_y(
            self.training_predictors, label)
        print "test :"
        self.X_test, _, self.predicition_date = self._to_X_y(
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

        print "----------START-data[predictors] ---------"
        print data[features].values.shape
        print "---------END-----------"

        if label:
            print "----------START-data[label]---------"
            print data[label].values.shape
            print "---------END-----------"
            return data[features].values, data[label].values, data['date'].values
        else:
            return data[features].values, 0, data['date'].values

    def make_prediction(self, site_id, label):
        self._create_X_Y_per_site(site_id, label)
        clf_RDM = {'params': {'n_estimators': [300], 'bootstrap': [
            True], 'criterion': ['mse']}, 'clf': RandomForestRegressor()}

        clf = clf_RDM['clf']
        params = clf_RDM['params']
        clf_rdm, Xtrain, ytrain, Xtest, ytest = \
            do_classify(clf, params, self.X_training, self.y)

        prediction = clf_rdm.predict(self.X_test)
        print prediction[-100:]

        self.forecast_predictors[label] = pd.Series(
            prediction, index=self.forecast_predictors.index)
        # print self.forecast_predictors.tail(100)

        self.forecast_predictors.to_csv("data/store/" + self.name +
                                        "_predictions_" + self.datastore.period
                                        + "_" + label + ".csv", sep=";")

    def export_to_json(self):
        pass
