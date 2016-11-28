import app.business_logic.model.merger.abstract as abstract


class CountsMerger(abstract.Merger):

    def __init__(self):
        super(CountsMerger, self).__init__(name="counts",
                                           left_keys=['idbldsite'], right_keys=['idbldsite'], suffixes=['_sites', '_counts'])
        self.filter_columns = ['idbldsite',
                               'compensatedin',
                               'date',
                               'date_time',
                               'latitude',
                               'longitude']

    def _set_right_data(self):
        self.right = self.datastore.get_counts()
