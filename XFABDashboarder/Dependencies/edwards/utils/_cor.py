import scipy.stats


def cor(x, y):
    """Calculate a Spearman correlation coefficient."""
    return scipy.stats.spearmanr(x, y).correlation
