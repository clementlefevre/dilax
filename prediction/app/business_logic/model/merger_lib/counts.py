import app.business_logic.model.merger as merger
from app.business_logic.service.counts_service import get_counts


class CountsMerger(merger.Merger):

    def __init__(self):
        super(CountsMerger, self).__init__("count_merger",
                                           "left", True,
                                           ['idbldsite'], ['idbldsite'])
        self.filter_columns = ['idbldsite',
                               'compensatedin',
                               'date',
                               'date_time',
                               'latitude',
                               'longitude']

    def _merge(self):
        df_sites = self.datastore.db.sites

        if self.datastore.retrocheck:

            df_counts = get_counts(
                self.datastore, date_to=self.datastore.date_from)

        else:
            df_counts = get_counts(self.datastore)

        self.merged = merger.pd.merge(df_sites, df_counts,
                                      on=['idbldsite'],
                                      suffixes=['sites_', 'counts'],
                                      indicator=True, how=self.how)
