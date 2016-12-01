from ..model.merger import *

countsMerger = counts.CountsMerger()
publicHolidaysMerger = public_holidays.PublicHolidaysMerger()
weatherObservationsDayMerger = weather_observations.WeatherObservationsDayMerger()
weatherObservationsHourMerger = weather_observations.WeatherObservationsHourMerger()
weatherForecastsDayMerger = weather_forecasts.WeatherForecastsDayMerger()
weatherForecastsHourMerger = weather_forecasts.WeatherForecastsHourMerger()
regionsMerger = regions.RegionsMerger()
schoolHolidaysMerger = school_holidays.SchoolHolidaysMerger()
datesMerger = dates.DatesMerger()
conversionDayMerger = conversion.ConversionDayMerger()


def merge_all_training(datastore):
    merged = _merge_with_counts(datastore)
    merged = _merge_with_weather_observations(datastore, merged)
    merged = _merge_with_public_holidays(datastore, merged)
    merged = _merge_with_regions(datastore, merged)
    merged = _merge_with_school_holidays(datastore, merged)
    merged = _merge_with_conversion(datastore, merged)

    return merged


def merge_all_forecasts(datastore):
    merged = _merge_with_dates(datastore)
    merged = _merge_with_public_holidays(datastore, merged)
    merged = _merge_with_school_holidays(datastore, merged)
    merged = _merge_with_weather_forecasts(datastore, merged)
    print merged.head()

    return merged


def _merge_with_counts(datastore):
    countsMerger.set_datastore(datastore)
    countsMerger.merge_and_clean(datastore.db_manager.sites)
    return countsMerger.merged


def _merge_with_dates(datastore):
    datesMerger.set_datastore(datastore)
    datesMerger.merge_and_clean(datastore.data.train.set)
    return datesMerger.merged


def _merge_with_weather_observations(datastore, merged):
    if datastore.period == 'D':
        weatherObservationsDayMerger.set_datastore(datastore)
        weatherObservationsDayMerger.merge_and_clean(merged)
        return weatherObservationsDayMerger.merged

    if datastore.period == 'H':
        weatherObservationsHourMerger.set_datastore(datastore)
        weatherObservationsHourMerger.merge_and_clean(merged)
        return weatherObservationsHourMerger.merged


def _merge_with_weather_forecasts(datastore, merged):
    if datastore.period == 'D':
        weatherForecastsDayMerger.set_datastore(datastore)
        weatherForecastsDayMerger.merge_and_clean(merged)

        return weatherForecastsDayMerger.merged

    if datastore.period == 'H':
        weatherForecastsHourMerger.set_datastore(datastore)
        weatherForecastsHourMerger.merge_and_clean(merged)
        return weatherForecastsHourMerger.merged


def _merge_with_public_holidays(datastore, merged):
    publicHolidaysMerger.set_datastore(datastore)
    publicHolidaysMerger.merge_and_clean(merged)
    return publicHolidaysMerger.merged


def _merge_with_regions(datastore, merged):
    regionsMerger.set_datastore(datastore)
    regionsMerger.merge_and_clean(merged)
    return regionsMerger.merged


def _merge_with_school_holidays(datastore, merged):
    schoolHolidaysMerger.set_datastore(datastore)
    schoolHolidaysMerger.merge_and_clean(merged)
    return schoolHolidaysMerger.merged


def _merge_with_conversion(datastore, merged):
    if datastore.period == 'D':
        conversionDayMerger.set_datastore(datastore)
        conversionDayMerger.merge_and_clean(merged)
        return conversionDayMerger.merged

    if datastore.period == 'H':
        pass
