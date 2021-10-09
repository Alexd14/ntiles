import pandas as pd
import numpy as np

import matplotlib as mpl
import matplotlib.pyplot as plt
from IPython.core.display import display

RETURN_COLOR_MAP = mpl.cm.get_cmap('jet')
TILTS_COLOR_MAP = mpl.cm.get_cmap('tab20')


def ntile_return_plot(cum_ntile_returns: pd.DataFrame, title):
    """
    generates cumulative return plot for a ntiles returns series
    if cols are empty list returns None
    :param cum_ntile_returns: cumulative returns we want to plot
    :param title: title of the plot
    :return: matplotlib axis with the return plot on it
    """

    _, ax = plt.subplots(1, 1, figsize=(10, 6))

    cum_ntile_returns.plot(lw=2, ax=ax, cmap=RETURN_COLOR_MAP)
    ax.set(ylabel='Log Cumulative Returns', title=title, xlabel='',
           yscale='symlog')

    ax.legend(loc="center left", bbox_to_anchor=(1, .5))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%Y'))
    ax.yaxis.set_major_formatter(mpl.ticker.FormatStrFormatter('%.2f'))
    ax.axhline(1, linestyle='-', color='black', lw=1)

    plt.show()
    return ax


def ntile_annual_return_bars(avg_annual_ret: pd.Series, period: int):
    """
    generates a box plot of the yearly CAGR for each ntile
    :return: matplotlib axis
    """
    num_ntiles = len(avg_annual_ret)

    _, ax = plt.subplots(1, 1, figsize=(9, 4.5))
    ax.set(ylabel='% Return', title=f'Annual Return, {period} Day Holding period', xlabel='')

    colors = [RETURN_COLOR_MAP(i) for i in np.linspace(0, 1, num_ntiles)]
    ax.bar(avg_annual_ret.index, avg_annual_ret.to_numpy(), color=colors)
    ax.axhline(0, linestyle='-', color='black', lw=1)

    plt.show()
    return ax


def plot_inspection_data(table, title, ylabel, decimals=0) -> None:
    """
    plots the inspection data for inspection tear sheets
    :param table: the table to plot
    :param title: the title for the plot
    :param ylabel: y label for plot
    :param decimals: amount of decimals to display on the Y axis
    :return: None
    """

    _, ax = plt.subplots(1, 1, figsize=(9, 4.5))
    ax.set(title=title, ylabel=ylabel)
    table.plot(lw=2, ax=ax, cmap=RETURN_COLOR_MAP)
    ax.legend(loc="center left", bbox_to_anchor=(1, .5))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%Y'))
    ax.yaxis.set_major_formatter(mpl.ticker.FormatStrFormatter(f'%.{decimals}f'))

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
        _, ax = plt.subplots(1, 1, figsize=(9, 4.5))

    ax.set(title=f'{ntile}, {group_name}'.title(), ylabel='Weight In Ntile')
    frame.plot(lw=2, ax=ax, cmap=TILTS_COLOR_MAP, legend=None)
    ax.axhline(0, linestyle='-', color='black', lw=1)
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%Y'))
    ax.yaxis.set_major_formatter(mpl.ticker.FormatStrFormatter(f'%.2f'))
    plt.show()


def plot_tilt_hist(series, ntile: str, group_name: str, extra_space=True):
    """
    Plots the histogram group tilts for a single ntile
    :param series: frame containing the avg tilts, columns: group, index: pd.Period
    :param ntile: the Ntile we are plotting for
    :param group_name: the name of the group
    :return: None
    """
    if extra_space:
        _, ax = plt.subplots(1, 2, figsize=(12, 4.5))
    else:
        _, ax = plt.subplots(1, 1, figsize=(4.5, 4.5))

    plotter_frame = series.to_frame('weight')
    plotter_frame['colors'] = [TILTS_COLOR_MAP(i) for i in np.linspace(0, 1, len(series))]
    plotter_frame = plotter_frame.sort_values('weight')

    ax[0].barh(plotter_frame.index.tolist(), plotter_frame['weight'].tolist(), align='center',
               color=plotter_frame['colors'].tolist())
    ax[0].set(title=f'{ntile}, {group_name}'.title(), ylabel='Group', xlabel='Weight Relative to Universe')
    ax[0].axhline(0, linestyle='-', color='black', lw=1)

    if extra_space:
        return ax[1]

    plt.show()


def plot_timeseries_ic(ic_series: pd.Series, holding_period):
    """
    plots the daily time series IC
    :param ic_series: series of IC to plot
    :return: None
    """
    ic_frame = ic_series.to_frame('IC')
    ic_frame['1 Month Avg IC'] = ic_frame.rolling(21).mean()

    _, ax = plt.subplots(1, 1, figsize=(9, 4))
    ic_frame.plot(ax=ax, title=f'IC {holding_period} Day Holding Period')
    ax.get_lines()[1].set_linewidth(3)
    ax.axhline(0, linestyle='-', color='black', lw=1)
    plt.show()


def render_heat_table(frame) -> None:
    """
    renders a dataframe as a heatmap
    :param frame: the frame to render
    :return: None
    """
    cm = mpl.cm.get_cmap('RdYlGn')
    styled = frame.style.background_gradient(cmap=cm, axis=0).format(precision=2).set_properties(
        **{'text-align': 'center'})
    render_table(styled)


def render_table(table, output=None) -> None:
    """
    displays a table to the user
    :param table: the table to display
    :return: None
    """
    if output:
        print(output)
    display(table)
