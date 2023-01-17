"""Plot individual chart - one system with multiple parameters."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>

import os
import warnings
from math import sqrt, ceil
import matplotlib.pyplot as plt
import pandas as pd


def plot_individual(data: pd.DataFrame,
                    sel_parametername: str | list | tuple | dict = None,
                    sel_parameternumber: int | list | tuple = None,
                    nrows: int = None,
                    ncols: int = None,
                    figsize: (float, float) = None,
                    title: str = None,
                    subtitle: str | list | tuple = None,
                    ylabel: str | list | tuple = None,
                    remove_yticklabels: bool = False,
                    sharex: bool | str = False,
                    sharey: bool | str = False,
                    xlim: dict | list | tuple = None,
                    ylim: dict | list | tuple = None,
                    path: str = None,
                    subplot_kw=None,
                    gridspec_kw=None,
                    **fig_kw):
    """
    Plot individual chart - one system with multiple parameters.

    Parameters
    ----------
    data : pd.DataFrame
        pd.DataFrame index is datetime type, while each column is a unique
        parameter with parameter name as column name.
    sel_parametername : str, list/tuple of str, dict, optional
        If str or list/tuple of str, `sel_parametername` selects the
        column/columns of `data` to be plotted.
        If dict, `sel_parametername` provides key (parameter number) & value (
        parameter name) pairs, and `sel_parameternumber` will select columns
        of `data` to be plotted by specifying parameter numbers.
        Default None, select all columns.
    sel_parameternumber : int, list/tuple of int, optional
        Default None.
    nrows, ncols : int, optional
        Number of rows and columns of the subplot grid.
        Default None, automatically configured.
    figsize: (float, float), optional
        Width, height in inches.
        Default None: rcParams["figure.figsize"] (default: [6.4, 4.8])
    title : str, optional
        Figure title.
        Default None, no title.
    subtitle : str, or list/tuple of str, optional
        Subplot titles.
        If str, all subplots use same `subtitle`.
        If list/tuple of str, length of `subtitle` must be same as
        that of selected columns.
        Default None, no subtitle.
    ylabel : str, list, or tuple, optional
        Subplot ylabel.
        If ylabel is str ending with ' i', the ending 'i' will be replaced
        with index of the subplot (index starting from 1).
        If list/tuple of str, length of `ylabel` must be same as that of
        selected columns.
        Default None, use names from `columns`.
    remove_yticklabels : bool, default: False
        Whether remove all yticklabels.
    sharex, sharey : bool or {'none', 'all', 'row', 'col'}, default: False
        Controls sharing of properties among x (sharex) or y (sharey)
        axes. See matplotlib.pyplot.subplots for details.
    xlim : dict, [xmin, xmax], or (xmin, xmax), optional
        Control subplot xlim.
        If dict, key is index of subplot (starting from 1), value is [xmin,
        xmax] of the subplot.
    ylim : dict, [ymin, ymax], or (ymin, ymax), optional
        Control subplot ylim.
        If dict, key is index of subplot (starting from 1), value is [ymin,
        ymax] of the subplot.
    path: str, optional
        Folder path to store plot as 'data.png', or the full name (including
        folder path) of the .png.
    subplot_kw: dict, optional
        'Dict with keywords passed to the add_subplot call used to create
        each subplot'.
        See matplotlib.pyplot.subplots for details.
    gridspec_kw: dict, optional
        'Dict with keywords passed to the GridSpec constructor used to
        create the grid the subplots are placed on'.
        See matplotlib.pyplot.subplots for details.
    **fig_kw
        'All additional keyword arguments are passed to the
        matplotlib.pyplot.figure call'.
        See matplotlib.pyplot.subplots for details.

    Returns
    -------
    fig, axes

    Notes
    -----
    See Also plot_overview(), plot overview chart - one parameter with
    multiple systems.
    Both plot_individual() and plot_overview() require input `data` to be
    pd.DataFrame. However, the structure of the required pd.DataFrame is
    different. This is due to the uncertainty/inconsistency in
    parameter name, i.e., same parameter number may correspond to
    different parameter names, though they have exactly same meaning.
    """

    if data is None:
        return None

    if sel_parametername is None:
        sel_parametername = data.columns
    elif isinstance(sel_parametername, dict):
        if sel_parameternumber is None:
            sel_parametername = list(sel_parametername.values())
        else:
            sel_parametername = [sel_parametername[k]
                                 for k in list(sel_parameternumber)
                                 if k in sel_parametername.keys()]

    def layout(n):
        n2 = ceil(sqrt(n))
        if n2 * (n2 - 1) >= n:
            n1 = n2 - 1
        else:
            n1 = n2
        return n1, n2

    if (nrows is None) and (ncols is None):
        nrows, ncols = layout(len(sel_parametername))
    elif (nrows is not None) and (ncols is not None):
        if nrows * ncols < len(sel_parametername):
            nrows = ceil(len(sel_parametername) / ncols)
            warnings.warn(
                f'"ncols*nrows < len(columns)". nrows is changed to {nrows}.')
    elif nrows is None:
        nrows = ceil(len(sel_parametername) / ncols)
    elif ncols is None:
        ncols = ceil(len(sel_parametername) / nrows)

    if isinstance(ylabel, (list, tuple)):
        if len(ylabel) != len(sel_parametername):
            raise ValueError('len(ylabel) != len(sel_parametername)')

    if isinstance(subtitle, (list, tuple)):
        if len(subtitle) != len(sel_parametername):
            raise ValueError('len(subtitle) != len(sel_parametername)')

    fig, axes = plt.subplots(nrows=nrows,
                             ncols=ncols,
                             sharex=sharex,
                             sharey=sharey,
                             figsize=figsize,
                             subplot_kw=subplot_kw,
                             gridspec_kw=gridspec_kw,
                             **fig_kw)
    axes = axes.ravel()

    columns = data.columns

    for i in range(len(sel_parametername)):
        col = sel_parametername[i]

        available = False
        if col in columns:
            if not data[col].dropna().empty:
                available = True

        if available:
            axes[i].plot(data[col].dropna())

            # set subtitle
            if subtitle is not None:
                if isinstance(subtitle, (list, tuple)):
                    axes[i].set_title(subtitle[i])
                elif isinstance(subtitle, str):
                    axes[i].set_title(subtitle)

            # set ylabel
            if ylabel is None:
                axes[i].set_ylabel(col)
            elif isinstance(ylabel, (list, tuple)):
                axes[i].set_ylabel(ylabel[i])
            elif isinstance(ylabel, str):
                if ylabel.split(' ')[-1] == 'i':
                    axes[i].set_ylabel(f'{ylabel[:-2]} {i + 1}')
                else:
                    axes[i].set_ylabel(ylabel)

            # set yticklabels
            if remove_yticklabels:
                axes[i].set_yticklabels('')

            # set ylim
            if isinstance(ylim, (list, tuple)):
                axes[i].set_ylim(ylim[0], ylim[1])
            elif (not sharey) & isinstance(ylim, dict):
                # Although axes index starts from 0, it makes more sense to
                # start name-related index (e.g., parameter name,
                # hence xlim and ylim) from 1.
                if i + 1 in ylim.keys():
                    axes[i].set_ylim(ylim[i + 1][0], ylim[i + 1][1])

            # set xlim
            if isinstance(xlim, (list, tuple)):
                axes[i].set_xlim(xlim[0], xlim[1])
            elif (not sharey) & isinstance(xlim, dict):
                if i + 1 in xlim.keys():
                    axes[i].set_xlim(xlim[i + 1][0], xlim[i + 1][1])
        else:
            axes[i].set_xticklabels('')
            axes[i].set_yticklabels('')

    for i in range(len(columns), ncols * nrows):
        axes[i].axis("off")

    # set title
    fig.suptitle(title)
    fig.tight_layout()

    if path is not None:
        if path.endswith('.png'):
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            fig.savefig(path)
        else:
            if not os.path.exists(path):
                os.makedirs(path)
            fig.savefig(f'{path}/individual.png')

    return fig, axes
