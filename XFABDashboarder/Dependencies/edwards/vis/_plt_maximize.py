"""
Maximize plot

@author: Danny Sun
"""

import matplotlib.pyplot as plt


def plt_maximize():
    """Maximize plot window."""
    backend = plt.get_backend()
    cfm = plt.get_current_fig_manager()
    if backend == 'TkAgg':
        cfm.resize(*cfm.window.maxsize())
    elif backend == 'wxAgg':
        cfm.frame.Maximize(True)
    elif backend == 'Qt5Agg':
        cfm.window.showMaximized()
