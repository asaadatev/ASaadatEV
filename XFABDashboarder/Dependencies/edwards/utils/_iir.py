import copy


def iir(x, alpha):
    """IIR filter."""

    y = copy.deepcopy(x)
    if len(x) > 1:
        y[0] = x[0]
        for i in range(1, len(x)):
            y[i] = (1 - alpha) * y[i - 1] + alpha * x[i]
    return y
