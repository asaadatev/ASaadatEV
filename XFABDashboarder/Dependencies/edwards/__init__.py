"""

"""

from ._ets import Ets

COLOR_ALERT = {'advisory': '#CDCDFF',
               'warning': '#FEFFA3',
               'alarm': '#FFB973'}

COLOR_HLINE = {
    'g': 'green', 'green': 'green',
    'b': 'blue', 'blue': 'blue', 'advisory': 'blue',
    'y': 'yellow', 'yellow': 'yellow', 'warning': 'yellow',
    'r': 'red', 'red': 'red', 'alarm': 'red',
}

__all__ = [
    'Ets',
    COLOR_ALERT,
    COLOR_HLINE,
]
