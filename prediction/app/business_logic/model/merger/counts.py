import app.business_logic.model.merger.abstract as abstract


class CountsMerger(abstract.Merger):

    def __init__(self):
        super(CountsMerger, self).__init__(name="counts",
                                           left_on=['idbldsite'], right_on=['idbldsite'], suffixes=['_sites', '_counts'])
        self.filter_columns = ['idbldsite',
                               'compensateditotalin',
                               'date',
                               'date_time',
                               'latitude',
                               'longitude']

        self.is_observed = False

    def _set_right_data(self):
        if self.is_observed:
            self.right = self.datastore.get_counts_observed()
        else:
            self.right = self.datastore.get_counts()
