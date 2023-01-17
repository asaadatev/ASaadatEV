"""
Read .parquet file and plot interactive .html graph.

@author: Danny Sun
"""

from datetime import datetime
import math
import os

import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots


def plot_model(filename, ncol, nrow=None, path=None, auto_open=True):
    """Generate interactive .html graph using plotly from .parquet file.

    Parameters
    ----------
    filename : str
        Name or full name of .parquet file
    ncol : int
        Number of columns in each .html graph
    nrow : int, optional
        Number of rows in each .html graph, by default None
        If None, the minimum to fit into one graph
    path : str, optional
        Path to save generated .html, by default None
    auto_open : bool, optional
        Whether open generated .html

    Returns
    -------
    List of plotly graph objects
    """
    data = pd.read_parquet(filename, engine='auto')

    subplot = list(dict.fromkeys(data['subplot'].values).keys())

    index = [list(data['subplot'].values).index(i) for i in subplot]
    system_names = list(data['system'].values[index])

    if nrow is None:
        nrow = int(len(subplot)/ncol)
        nfig = 1
    else:
        nfig = int(math.ceil(len(subplot)/(ncol*nrow)))

    fig = list()
    for i in range(nfig):
        fig.append(make_subplots(rows=nrow,
                                 cols=ncol,
                                 subplot_titles=system_names[
                                     (i*nrow*ncol):((i+1)*nrow*ncol)]))

    hline_color = {'r': 'red', 'red': 'red',
                   'g': 'green', 'green': 'green',
                   'b': 'blue', 'blue': 'blue',
                   'y': 'yellow', 'yellow': 'yellow'}

    alert_color = {'advisory': 'blue',
                   'warning': 'green',
                   'alarm': 'red'}

    for i_subplot in range(len(subplot)):

        data_i = data.loc[data.loc[:, 'subplot'] == subplot[i_subplot], :]
        parameter = list(dict.fromkeys(data_i['parameter'].values).keys())

        i_fig = i_subplot//(ncol*nrow)

        if ((i_subplot+1) % (ncol*nrow)) == 0:
            u_row = nrow
        else:
            u_row = ((i_subplot-i_fig*(ncol*nrow))//ncol) + 1

        v_col = ((i_subplot-i_fig*(ncol*nrow)) % ncol) + 1

        for j_parameter in range(len(parameter)):

            data_i_j = data_i.loc[data_i.loc[:, 'parameter'] ==
                                  parameter[j_parameter], :]

            alert = data_i_j.loc[:, ['alert', 'start', 'end']].copy()
            alert = alert.dropna(how='any')

            hline = data_i_j.loc[:, ['hline_color', 'hline_value']].copy()
            hline = hline.dropna(how='any')

            fig[i_fig].add_trace(go.Scatter(x=data_i_j.loc[:, 'datetime'],
                                            y=data_i_j.loc[:, 'value'],
                                            mode='lines',
                                            line_width=1,
                                            line_dash="solid",
                                            line_color="blue"),
                                 row=u_row,
                                 col=v_col)

            fig[i_fig].update_xaxes(title_text='', row=u_row, col=v_col)
            fig[i_fig].update_yaxes(
                title_text=parameter[j_parameter], row=u_row, col=v_col)

            for k in range(alert.shape[0]):
                fig[i_fig].add_vrect(
                    x0=datetime.utcfromtimestamp(
                        alert['start'].values[k].tolist()/1e9),
                    x1=datetime.utcfromtimestamp(
                        alert['end'].values[k].tolist()/1e9),
                    fillcolor=alert_color[alert['alert'].values[k]],
                    opacity=0.5,
                    layer="below",
                    line_width=0,
                    row=u_row,
                    col=v_col)

            for k in range(hline.shape[0]):
                fig[i_fig].add_hline(
                    y=hline['hline_value'].values[k],
                    line_width=0.5,
                    line_dash="dash",
                    line_color=hline_color[hline['hline_color'].values[k]],
                    row=u_row,
                    col=v_col)

            fig[i_fig].update_layout(showlegend=False)

    if path is not None:
        if not os.path.exists(path):
            os.makedirs(path)

        filename_base = os.path.basename(filename)

        name = os.path.splitext(filename_base)[0]

        if len(fig) == 1:
            fig[0].write_html(f'{path}/{name}.html',
                              auto_open=auto_open)
        else:
            for i, f in enumerate(fig, start=1):
                f.write_html(f'{path}/{name}_{i}.html',
                             auto_open=auto_open)

    return fig
