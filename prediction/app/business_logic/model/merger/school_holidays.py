import app.business_logic.model.merger.abstract as abstract


class SchoolHolidaysMerger(abstract.Merger):

    def __init__(self):
        super(SchoolHolidaysMerger, self).__init__(name="public_holidays",
                                                   left_keys=[
                                                       'region_id', 'date'],
                                                   right_keys=[
                                                       'region_id', 'date'],
                                                   suffixes=['_data', '_school'])
        self.filter_columns = None

    def _set_right_data(self):
        df_school_holidays = abstract.pd.read_csv(abstract.get_file_path(
            '../data/school_holidays.csv', abstract.fileDir), parse_dates=['date'])

        self.right = df_school_holidays
