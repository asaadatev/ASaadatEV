import numpy as np


def centroid(x, f,
             power: int = 1):
    """
    Calculate weighted average frequency.

    Parameters
    ----------
    x : Array of frequency amplitudes
    f : Array of sample frequencies
    power : 1 or 2
      1: weighted average frequency with frequency amplitudes as weight
      2: weighted average frequency with power amplitudes as weight
    """
    return np.sum(np.power(x, power) * f) / np.sum(np.power(x, power))
