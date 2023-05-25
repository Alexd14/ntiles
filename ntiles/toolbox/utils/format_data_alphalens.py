import pandas as pd

from ntiles.toolbox.utils.handle_data import handle_duplicates, make_nan_inf_summary


def price_format_for_alphalens(data: pd.DataFrame, factor: str, date_format: str = '',
                               id_col: str = 'symbol') -> pd.DataFrame:
    """
    formats the price data into the expected format by get_clean_factor_and_forward_returns
    out format of the data frame: index: 'date', columns: id_col
    data must contain 'date', id_col, can take in a dataframe with unlimited columns
    given df the 2 required columns names: 'date', id_col

    does not mutate the given dataframe

    :param data: the data to be turned into the format expected by prices field in get_clean_factor_and_forward_returns
    :param factor: the name of the factor column in the passed data
    :param date_format: the format to parse the date column in pd.datetime
    `   dont pass anything if no date conversion is wanted
    :param id_col: the asset identifier column for the data
    :return: data frame with data in format required by factor field in get_clean_factor_and_forward_returns
    """
    data: pd.DataFrame = data.copy()

    _check_columns(data, id_col)
    _convert_to_date_time(data, date_format)

    pivot_table: pd.DataFrame = data.pivot_table(index='date', columns=id_col, values=factor)

    return pivot_table


def factor_format_for_alphalens(data: pd.DataFrame, factor: str, date_format: str = '', max_loss: float = .1,
                                id_col: str = 'symbol') -> pd.DataFrame:
    """
    formats the alpha factor data into the expected format by get_clean_factor_and_forward_returns
    data must contain 'date', id_col, can take in a dataframe with unlimited columns
    out format of the data frame: index: ('date', id_col), columns: 'factor'
    given df the 1 required columns names: 'date'

    does not mutate the given data frame

    :param data: the data to be turned into the format expected by factor field in get_clean_factor_and_forward_returns
    :param factor: the name of the factor column in the passed data
    :param date_format: the format to parse the date column in pd.datetime
    `   pass nothing if no date conversion is wanted
    :param max_loss: the decimal percent of the factor that can be nan or infinity before we throw an error
    :param id_col: the asset identifier column for the data
    :return: data frame with data in required format by factor field in get_clean_factor_and_forward_returns
    """
    data: pd.DataFrame = data.copy()

    _check_columns(data, id_col)
    _convert_to_date_time(data, date_format)

    # setting the index
    alpha_factor = data[['date', id_col, factor]].set_index(['date', id_col])
    # dropping duplicates and printing a warning
    alpha_factor = handle_duplicates(df=alpha_factor, out_type='Warning', name='Given Factor', drop=True)
    # making a nan and inf summary along with dropping nan's
    alpha_factor = make_nan_inf_summary(df=alpha_factor, max_loss=max_loss)

    return alpha_factor


def _check_columns(data: pd.DataFrame, id_col: str) -> None:
    """
    checking to make sure the columns contain 'date' & id_col
    :param data: the data frame to check
    :param id_col: the identifier column we are checking for
    :return: Void, throws ValueError if the columns are bad
    """
    # checking for the columns 'date' & id_col
    for needed in ['date', id_col]:
        if needed not in data.columns:
            raise ValueError(f'given df must have required columns \'date\' \'{id_col}\'')


def _convert_to_date_time(data: pd.DataFrame, date_format: str) -> None:
    """
    MUTATES the given dataframe
    converts the date column to a pd.dateTime object.
    If the date_format is a empty string then nothing is changed
    :param data: the data frame to have the date chamged
    :param date_format: the format of the date time string
    :return: Void
    """

    if date_format != '':
        data['date'] = pd.to_datetime(data['date'].to_numpy(), format=date_format, utc=True)
