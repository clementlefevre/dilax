import app.business_logic.model.datastore.datastore as datastore


class DayDatastore(datastore.Datastore):

    PREDICT_RANGE_DAYS = 30

    def _get_period(self):
        return "day"

    def _training_set_(self):
        if not self.file_exists('training_set'):
            self.create_training_set()
        else:
            self.training_set = self.read_file('training_set')

    def _create_sites_dict(self):
        pass

    def _get_observed_target(self):
        pass

    def _create_forecasts(self):
        pass
