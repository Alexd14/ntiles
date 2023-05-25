from typing import List, Union

import pandas as pd


class DateConfig:
    """
    Configures the dates for a dataframe

    This class can be used to align dates up across pricing and factor data
    Once the class is configured can be used an unlimited number of times to align dates of dataframes
    """

    def __init__(self,
                 freq: str,
                 target_data_type: str = 'period',
                 resample: bool = False,
                 resample_key: str = None,
                 grouper_keys: List[str] = None,
                 date_format: str = None
                 ) -> None:
        """
        :param freq: the frequency we want to align to
        :param target_data_type: the type of date we want as output (timestamp, period)
        :param resample: whether to resample the data when changing frequency's
        :param resample_key: The column we are using to down sample the data
            Will keep the last value of the period
        :param grouper_keys: The columns we are using to group the data
            will be used in conjunction with resample_key
            can be none if we are not grouping
            This would be where asset_ids go
        :param date_format: the date format to use when converting from a string to period
        """

        self._target_freq = freq
        self._target_data_type = target_data_type
        self._date_format = date_format
        self._resample = resample
        self._resample_key = resample_key
        self._grouper_keys = [] if grouper_keys is None else grouper_keys
        self._resample_master = f'old_{self._resample_key}_{self.__class__.__name__}'
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        """
        Ensures the inputs are valid
        :throws: ValueError if inputs are invalid
        """
        if self._target_freq not in ['D', 'B', 'W', 'M', 'Q', 'A']:
            raise ValueError(f'Invalid target_freq: {self._target_freq}')

        # if not self._resample and len(self._grouper_keys) != 0:
        #     raise ValueError(f'Cannot use grouper_keys without resampling')

        if self._target_data_type not in ['timestamp', 'period']:
            raise ValueError(f'Invalid target_data_type: {self._target_data_type}')

    def configure_dates(self,
                        df: pd.DataFrame,
                        date_columns: Union[List[str], str]
                        ) -> pd.DataFrame:
        """
        Adjusts the dates of the dataframe according to the configuration passed at initiation
        Cn adjust columns as well as the index
        :param df: the dataframe to adjust
        :param date_columns: the date columns to adjust
        :return: the dataframe with the configured dates
        """
        df = df.copy()

        if isinstance(date_columns, str):
            date_columns = [date_columns]

        index = None
        if not isinstance(df.index, pd.RangeIndex):
            index = df.index.name
            df = df.reset_index()

        self._validate_df(df, date_columns)
        df = self._prep_df(df, date_columns)
        for date_column in date_columns:
            df[date_column] = self._configure_dates(df[date_column])
        df = self._resample_data(df, date_columns)
        df = self._clean_df(df)
        df = self._alter_types(df, date_columns)

        if index:
            df = df.set_index(index)
        return df

    def _alter_types(self,
                     df: pd.DataFrame,
                     date_columns: Union[List[str], str]
                     ) -> pd.DataFrame:
        """
        Alters the types of the dates to the target_data_type
        """
        if self._target_data_type == 'timestamp':
            for date_column in date_columns:
                df[date_column] = df[date_column].dt.to_timestamp()
        return df

    def _clean_df(self,
                  df: pd.DataFrame
                  ) -> pd.DataFrame:
        """
        Cleans the df after the dates have been adjusted
        """
        return df.drop(self._resample_master, axis=1, errors='ignore')

    def _prep_df(self,
                 df: pd.DataFrame,
                 date_columns
                 ) -> pd.DataFrame:
        """
        Preps the df for the dates to be adjusted
        Currently preps for a frequency conversion and subsequent down-sample or up-sample
        :throws: ValueError if the correct parameters are not passed at construction to do the resample
        """

        if self._resample:
            if self._resample_key is None:
                df[self._resample_master] = df[date_columns[0]]
            else:
                df[self._resample_master] = df[self._resample_key]
        return df

    def _resample_data(self,
                       df: pd.DataFrame,
                       date_columns
                       ) -> pd.DataFrame:
        """
        Upsamples the data if resample is True
        """
        if self._resample and len(self._grouper_keys) == 0 and len(df) > 10_000:
            print('Warning you are resampling a large dataframe without grouping.')

        if self._resample:
            date_key = date_columns[0] if self._resample_key is None else self._resample_key
            groupby_keys = self._grouper_keys.copy() + [date_key]
            df = df.sort_values(self._resample_master).groupby(groupby_keys).last().reset_index()

        return df

    def _validate_df(self,
                     df,
                     date_columns
                     ) -> None:
        """
        Ensures the inputs are valid for down sampling
        :throws: ValueError if inputs are invalid
        """
        if self._resample and len(date_columns) != 1 and self._resample_key is None:
            raise ValueError(f'Cannot down sample multiple date columns: {date_columns}. Must pass resample_key.')

        if self._resample and self._resample_key is not None and self._resample_key not in date_columns:
            raise ValueError(f'resample_key: {self._resample_key} not in date_columns: {date_columns}')

    def _configure_dates(self,
                         dates: pd.Series
                         ) -> pd.Series:
        """
        Adjusts the dates according to the configuration passed at initiation
        """
        if not (pd.api.types.is_period_dtype(dates) or pd.api.types.is_datetime64_any_dtype(dates)):
            dates = self._to_datetime(dates)

        dates = self._configure_freq(dates)
        return dates

    def _configure_freq(self,
                        dates: pd.Series
                        ) -> pd.Series:
        """
        Configures the frequency of the dates
        """
        if pd.api.types.is_datetime64_any_dtype(dates):
            if dates.dt.tz:
                dates = dates.dt.tz_localize(None)
            return dates.dt.to_period(self._target_freq)
        if pd.api.types.is_period_dtype(dates):
            if dates.dt.freq != self._target_freq:
                return dates.dt.asfreq(self._target_freq)
            else:
                return dates
        else:
            raise ValueError(f'Invalid date date type: {dates.dtype}')

    def _to_datetime(self,
                     dates
                     ) -> pd.Series:
        """
        Takes in a series of strings and parses them to dates
        :throws: ValueError if date_format is not passed at initiation
        """
        if self._date_format is None:
            raise ValueError('date_format must be passed at initiation to parse dates from strings')
        return pd.to_datetime(dates, format=self._date_format)

    def copy(self,
             **kwargs
             ) -> 'DateConfig':
        """
        Creates a copy of the object
        :param kwargs: the parameters to override when doing the copy
        """
        base_kwargs = {'freq': self._target_freq,
                       'date_format': self._date_format,
                       'target_data_type': self._target_data_type,
                       'resample': self._resample,
                       'resample_key': self._resample_key,
                       'grouper_keys': self._grouper_keys}
        base_kwargs.update(kwargs)
        return self.__class__(**base_kwargs)
