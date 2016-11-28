import app.business_logic.model.datastore.datastore as datastore


class HourDatastore(datastore.Datastore):

    PREDICT_RANGE_DAYS = 30

    def _get_period(self):
        return "H"

    def _get_observed_target(self):
        pass

    def _create_forecasts(self):
        pass

    def _aggregate_counts(self, df_counts):
        df_counts['date_time'] = df_counts['timestamp'].apply(
            lambda ts: round_to_nearest_hour(ts))
        df_counts = df_counts.groupby(['idbldsite', 'date_time']).sum()
        df_counts = df_counts.reset_index()
        df_counts['date'] = df_counts['date_time'].apply(lambda dt:
                                                         datetime(dt.year,
                                                                  dt.month,
                                                                  dt.day))
        return df_counts
