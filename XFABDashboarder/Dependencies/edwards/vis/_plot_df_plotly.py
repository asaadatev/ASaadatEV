from datetime import datetime
import math
import os

import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

from .. import COLOR_ALERT, COLOR_HLINE


def plot_df_plotly(src, ncol=1, nrow=None, path=None, auto_open=True):
    """Generate .html using plotly from pandas.DataFrame or .parquet file.

    Parameters
    ----------
    src : str or pandas.DataFrame
        If str, full name of a .parquet file.
    ncol : int, optional
        Number of columns in each .html graph, default 1.
    nrow : int, optional
        Number of rows in each .html graph. Default None, the minimum number
        of rows needed to fit all subplots into one graph.
    path : str, optional
        Where to save generated .html.
        Default None, if `src` is pandas.DataFrame, `path` = working directory,
        if `src` is str, path = directory of `str`.
    auto_open : bool, optional
        Whether open generated .html, default True.

    Returns
    -------
    List of plotly graph objects.
    """

    if isinstance(src, pd.DataFrame):
        df = src
    elif isinstance(src, str):
        df = pd.read_parquet(src, engine='auto')
    else:
        raise TypeError('src must be DataFrame or name of a .parquet file!')

    subplot = list(df['subplot'].unique())
    # subplot = list(dict.fromkeys(df['subplot'].values).keys())

    if nrow is None:
        nrow = int(len(subplot) / ncol)
        nfig = 1
    else:
        nfig = int(math.ceil(len(subplot) / (ncol * nrow)))

    data = {}
    title = {}
    ylabel = {}
    alert = {}
    hline = {}

    for i, v in enumerate(subplot):
        data[i] = df.loc[df.loc[:, 'subplot'] == v, :]

        if len(data[i]['title'].dropna().unique()) == 0:
            title[i] = data[i]['system'].dropna().unique()[0]
        else:
            title[i] = data[i]['title'].dropna().unique()[0]

        if len(data[i]['ylabel'].dropna().unique()) == 0:
            ylabel[i] = data[i]['parameter'].dropna().unique()[0]
        else:
            ylabel[i] = data[i]['ylabel'].dropna().unique()[0]

        alert[i] = data[i].loc[:, ['alert', 'start', 'end']].copy()
        alert[i] = alert[i].dropna(how='any').drop_duplicates()
        hline[i] = data[i].loc[:, ['hline_color', 'hline_value']].copy()
        hline[i] = hline[i].dropna(how='any').drop_duplicates()

    fig = list()
    for i_fig in range(nfig):
        fig.append(make_subplots(
            rows=nrow,
            cols=ncol,
            subplot_titles=list(title.values())[(i_fig * nrow * ncol):
                                                ((i_fig + 1) * nrow * ncol)]))

    for i_subplot, _ in enumerate(subplot):

        # parameter = list(dict.fromkeys(data[i]['parameter'].values).keys())
        parameter = list(data[i_subplot]['parameter'].unique())

        i_fig = i_subplot // (ncol * nrow)

        if ((i_subplot + 1) % (ncol * nrow)) == 0:
            u_row = nrow
        else:
            u_row = ((i_subplot - i_fig * (ncol * nrow)) // ncol) + 1

        v_col = ((i_subplot - i_fig * (ncol * nrow)) % ncol) + 1

        for j_parameter, j_v in enumerate(parameter):

            ts = data[i_subplot].loc[data[i_subplot].loc[:, 'parameter']
                                     == j_v, :]

            format_j = data[i_subplot].loc[data[i_subplot].loc[:, 'parameter']
                                           == j_v, 'format'].dropna(how='any')

            # TODO: to support 'plot b', 'stairs +ob', 'bar g'
            if len(format_j) == 0:
                fig[i_fig].add_trace(go.Scatter(x=ts.loc[:, 'datetime'],
                                                y=ts.loc[:, 'value'],
                                                mode='lines',
                                                line_width=1,
                                                line_dash="solid",
                                                line_color="blue"),
                                     row=u_row,
                                     col=v_col)
            else:
                pass

        fig[i_fig].update_xaxes(title_text='', row=u_row, col=v_col)
        fig[i_fig].update_yaxes(title_text=ylabel[i_subplot],
                                row=u_row, col=v_col)

        for k in range(alert[i_subplot].shape[0]):
            fig[i_fig].add_vrect(
                x0=datetime.utcfromtimestamp(
                    alert[i_subplot]['start'].values[k].tolist() / 1e9),
                x1=datetime.utcfromtimestamp(
                    alert[i_subplot]['end'].values[k].tolist() / 1e9),
                fillcolor=COLOR_ALERT[alert[i_subplot]['alert'].values[k]],
                opacity=1,  # 0.5
                layer="below",
                line_width=0,
                row=u_row,
                col=v_col)

        for k in range(hline[i_subplot].shape[0]):
            fig[i_fig].add_hline(
                y=hline[i_subplot]['hline_value'].values[k],
                line_width=0.5,
                line_dash="dash",
                line_color=COLOR_HLINE.get(
                    hline[i_subplot]['hline_color'].values[k]),
                row=u_row,
                col=v_col)
        fig[i_fig].update_layout(showlegend=False)

    foldername = None
    filename = None

    if path is None:
        if isinstance(src, str):
            foldername = os.path.dirname(src)
            filename = os.path.splitext(os.path.basename(src))[0]
    else:
        if path.endswith('.html'):
            foldername = os.path.dirname(path)
            filename = os.path.splitext(os.path.basename(path))[0]
        else:
            foldername = path

    if foldername is None:
        foldername = os.getcwd()

    if filename is None:
        filename = 'temp'

    if not os.path.exists(foldername):
        os.makedirs(foldername)

    if len(fig) == 1:
        fig[0].write_html(f'{foldername}/{filename}.html',
                          auto_open=auto_open)
    else:
        for i, f in enumerate(fig, start=1):
            f.write_html(f'{foldername}/{filename}_{i}.html',
                         auto_open=auto_open)

    return fig
