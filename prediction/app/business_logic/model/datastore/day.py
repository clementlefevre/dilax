from app.business_logic.model.datastore.abstract import Datastore


class DayDatastore(Datastore):

    PREDICT_RANGE_DAYS = 30

    def _get_period(self):
        return "D"

    def _get_observed_target(self):
        pass

    def _create_forecasts(self):
        pass

    def _aggregate_counts(self, df_counts):
        df_counts = df_counts.groupby(['idbldsite', 'date']).sum()
        df_counts = df_counts.reset_index()
        df_counts['date_time'] = df_counts['date']

        return df_counts
