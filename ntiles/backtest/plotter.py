import pandas as pd
import numpy as np

import matplotlib as mpl
import matplotlib.pyplot as plt
from IPython.core.display import display

RETURN_COLOR_MAP = mpl.cm.get_cmap('jet')
TILTS_COLOR_MAP = mpl.cm.get_cmap('tab20')
IC_COLOR_MAP = mpl.cm.get_cmap('tab10')

LARGE_FIGSIZE = 20, 10
MEDIUM_FIGSIZE = 15, 8


def ntile_return_plot(cum_ntile_returns: pd.DataFrame, title: str):
    """
    generates cumulative return plot for a ntiles returns series
    if cols are empty list returns None
    :param cum_ntile_returns: cumulative returns we want to plot
    :param title: title of the plot
    :return: matplotlib axis with the return plot on it
    """

    fig, ax = plt.subplots(1, 1, figsize=LARGE_FIGSIZE)

    cum_ntile_returns.plot(lw=2, ax=ax, cmap=RETURN_COLOR_MAP)
    ax.set(ylabel='Log Cumulative Returns', title=title, xlabel='',
           yscale='symlog')

    ax.legend(loc="center left", bbox_to_anchor=(1, .5))
    ax.set_yscale('log', base=2)
    ax.yaxis.set_major_formatter(mpl.ticker.FormatStrFormatter('%.2f'))
    ax.axhline(1, linestyle='-', color='black', lw=1)
    fig.autofmt_xdate()

    plt.show()
    return ax


def ntile_annual_return_bars(avg_annual_ret: pd.Series, period: int, freq: str):
    """
    generates a box plot of the yearly CAGR for each ntile
    :return: matplotlib axis
    """
    num_ntiles = len(avg_annual_ret)

    _, ax = plt.subplots(1, 1, figsize=MEDIUM_FIGSIZE)
    ax.set(ylabel='% Return',
           title=f'Annual Return, {period}{freq} Holding period',
           xlabel='')

    colors = [RETURN_COLOR_MAP(i) for i in np.linspace(0, 1, num_ntiles)]
    ax.bar(avg_annual_ret.index, avg_annual_ret.to_numpy(), color=colors)
    ax.axhline(0, linestyle='-', color='black', lw=1)

    plt.show()
    return ax


def plot_inspection_data(table: pd.DataFrame, title: str, ylabel: str, decimals: int = 0) -> None:
    """
    plots the inspection data for inspection tear sheets
    :param table: the table to plot
    :param title: the title for the plot
    :param ylabel: y label for plot
    :param decimals: amount of decimals to display on the Y axis
    :return: None
    """

    fig, ax = plt.subplots(1, 1, figsize=MEDIUM_FIGSIZE)
    ax.set(title=title, ylabel=ylabel)
    table.plot(lw=2, ax=ax, cmap=RETURN_COLOR_MAP)
    ax.legend(loc="center left", bbox_to_anchor=(1, .5))
    # ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%Y'))
    ax.yaxis.set_major_formatter(mpl.ticker.FormatStrFormatter(f'%.{decimals}f'))
    fig.autofmt_xdate()

    if isinstance(table, pd.Series):
        ax.get_legend().remove()

    plt.show()


def plot_tilts(frame: pd.DataFrame, ntile: str, group_name: str, ax=None):
    """
    Plots the timeseries group tilts for a single ntile
    :param frame: frame containing the tilts per day, columns: group, index: pd.Period
    :param ntile: the Ntile we are plotting for
    :param group_name: the name of the group
    :param ax: axis to plot on
    :return: None
    """
    if ax is None:
        fig, ax = plt.subplots(1, 1, figsize=MEDIUM_FIGSIZE)

    ax.set(title=f'{ntile}, {group_name}'.title(), ylabel='Weight In Ntile')
    frame.plot(lw=2, ax=ax, cmap=TILTS_COLOR_MAP, legend=None)
    ax.axhline(0, linestyle='-', color='black', lw=1)
    # ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%Y'))
    ax.yaxis.set_major_formatter(mpl.ticker.FormatStrFormatter(f'%.2f'))
    plt.show()


def plot_tilt_hist(series, ntile: str, group_name: str, extra_space: bool = True):
    """
    Plots the histogram group tilts for a single ntile
    :param series: frame containing the avg tilts, columns: group, index: pd.Period
    :param ntile: the Ntile we are plotting for
    :param group_name: the name of the group
    :return: None
    """
    if extra_space:
        fig, ax = plt.subplots(1, 2, figsize=LARGE_FIGSIZE)
    else:
        _, ax = plt.subplots(1, 1, figsize=(4.5, 4.5))

    title = 'Weight Relative to Universe' if 'Ntile' in group_name else 'Group Exposure'
    plotter_frame = series.to_frame('weight')
    plotter_frame['colors'] = [TILTS_COLOR_MAP(i) for i in np.linspace(0, 1, len(series))]
    plotter_frame = plotter_frame.sort_values('weight')

    ax[0].barh(plotter_frame.index.tolist(), plotter_frame['weight'].tolist(), align='center',
               color=plotter_frame['colors'].tolist())
    ax[0].set(title=f'{ntile}, {group_name}'.title(), ylabel='Group', xlabel=title)
    ax[0].axvline(0, linestyle='-', color='black', lw=1)

    if extra_space:
        return ax[1]

    plt.show()


def plot_timeseries_ic(ic_frame: pd.DataFrame, holding_period: int):
    """
    plots the daily time series IC
    :param ic_frame: frame of IC to plot index: pd.Period
    :param holding_period: how long the holding period is for the IC
    :return: None
    """
    fig, ax = plt.subplots(1, 1, figsize=MEDIUM_FIGSIZE)
    ic_frame.plot(ax=ax, title=f'IC {holding_period} {ic_frame.index.freq.name} Holding Period')
    ax.get_lines()[1].set_linewidth(3)
    ax.axhline(0, linestyle='-', color='black', lw=1)
    fig.autofmt_xdate()
    plt.show()


def plot_auto_corr(ac_series: pd.Series, holding_period: int) -> None:
    """
    plots the daily time series IC
    :param ac_series: series of auto corr to plot index: pd.Period
    :param holding_period: how long the holding period is for the IC
    :return: None
    """
    fig, ax = plt.subplots(1, 1, figsize=MEDIUM_FIGSIZE)
    ac_series.plot(ax=ax, title=f'Autocorrelation {holding_period}{ac_series.index.freq.name} Holding Period')
    ax.axhline(ac_series.median(), linestyle=(0, (5, 10)), color='black', lw=1)
    fig.autofmt_xdate()
    plt.show()


def plot_turnover(turn_frame: pd.Series, holding_period: int) -> None:
    """
    plots the daily time series IC
    :param turn_frame: dataframe of turnover to plot index: pd.Period
    :param holding_period: how long the holding period is for the IC
    :return: None
    """
    fig, ax = plt.subplots(1, 1, figsize=MEDIUM_FIGSIZE)
    colors = [RETURN_COLOR_MAP(i) for i in np.linspace(0, 1, turn_frame.columns.max())]

    for col in turn_frame.columns:
        ax.plot(turn_frame.index.to_timestamp(), turn_frame[col], color=colors[col - 1], label=f'Ntile: {col}')
        ax.axhline(turn_frame[col].median(), linestyle=(0, (5, 10)), color=colors[col - 1], lw=5)

    ax.set(ylabel='% Turnover', title=f'Turnover {holding_period}{turn_frame.index.freq.name} Holding Period',
           xlabel='')
    ax.legend(loc="center left", bbox_to_anchor=(1, .5))
    fig.autofmt_xdate()
    plt.show()


def plot_ic_horizon(horizon_frame: pd.DataFrame):
    ax_tuple = plt.subplots(2, 2, figsize=LARGE_FIGSIZE)[1].flatten()
    colors = [IC_COLOR_MAP(i) for i in np.linspace(0, 1, 4)]

    for i in range(horizon_frame.shape[1]):
        plot_me = horizon_frame.iloc[:, i]
        plot_me.plot(ax=ax_tuple[i], color=colors[i], title=plot_me.name)
    plt.show()


def render_heat_table(frame: pd.DataFrame) -> None:
    """
    renders a dataframe as a heatmap
    :param frame: the frame to render
    :return: None
    """
    cm = mpl.cm.get_cmap('RdYlGn')
    styled = frame.style.background_gradient(cmap=cm, axis=0).format('{:.2f}').set_properties(
        **{'text-align': 'center'})
    render_table(styled)


def render_table(table: pd.DataFrame, output: str = None) -> None:
    """
    displays a table to the user
    :param table: the table to display
    :param output: the output we should render
    :return: None
    """
    if output:
        print(output)
    display(table)
