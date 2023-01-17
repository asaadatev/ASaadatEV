# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 09:58:34 2021

@author: Dennis Hou
"""
import numpy as np
import pandas as pd
import scipy
import cvxpy


def trend_filter(data, reg_norm=2, lambda_value=50, batch_size=100):
    """
    Hodrick-Prescott (H-P) version of trend filtering and L1 trend filtering

    :param data: pandas Series with datetime index to be filtered
    :param reg_norm: L1 or L2 norm of penalty for smoothness 
    :param lambda_value: regularisation paramter
    :param batch_size: number of data points to be process in one optimisation
    :return : filtered data array
    """
    values = data.values
    if batch_size:
        try:
            assert len(values) > batch_size
        except AssertionError:
            print('Data must be longer than batch_size')
        try:
            assert reg_norm in [1,2]
            if reg_norm == 1:
                solver = cvxpy.ECOS
            else:
                solver = cvxpy.CVXOPT
        except AssertionError:
            print('reg_norm should be either 1 or 2')
        batch = len(values) // batch_size
        remain = len(values) % batch_size

        X = np.array([])
        for b in range(batch):
            y = values[b * batch_size: (b + 1) * batch_size]
            n = y.size
            ones_row = np.ones((1, n))
            D = scipy.sparse.spdiags(np.vstack((ones_row, -2*ones_row, ones_row)), range(3), n-2, n)

            x = cvxpy.Variable(shape=n)     # x is the filtered trend that we initialize
            objective = cvxpy.Minimize(0.5 * cvxpy.sum_squares(y-x) + lambda_value * cvxpy.norm(D@x, reg_norm))    # Note: D@x is syntax for matrix multiplication
            problem = cvxpy.Problem(objective)
            problem.solve(solver=solver, verbose=False)

            X = np.append(X, np.array(x.value))

        y = values[(b + 1) * batch_size: (b + 1) * batch_size + remain]
        if len(y) > 2:
            n = y.size
            ones_row = np.ones((1, n))
            D = scipy.sparse.spdiags(np.vstack((ones_row, -2*ones_row, ones_row)), range(3), n-2, n)

            x = cvxpy.Variable(shape=n)     # x is the filtered trend that we initialize
            objective = cvxpy.Minimize(0.5 * cvxpy.sum_squares(y-x) + lambda_value * cvxpy.norm(D@x, reg_norm))    # Note: D@x is syntax for matrix multiplication
            problem = cvxpy.Problem(objective)
            problem.solve(solver=solver, verbose=False)
            X = np.append(X, np.array(x.value))
        else:
            X = np.append(X, X[-1])
    else:
        try:
            assert reg_norm in [1,2]
            if reg_norm == 1:
                solver = cvxpy.ECOS
            else:
                solver = cvxpy.CVXOPT
        except AssertionError:
            print('reg_norm should be either 1 or 2')
        X = np.array([])
        y = values
        n = y.size
        ones_row = np.ones((1, n))
        D = scipy.sparse.spdiags(np.vstack((ones_row, -2 * ones_row, ones_row)), range(3), n - 2, n)

        x = cvxpy.Variable(shape=n)  # x is the filtered trend that we initialize
        objective = cvxpy.Minimize(0.5 * cvxpy.sum_squares(y - x) + lambda_value * cvxpy.norm(D @ x,
                                                                                              reg_norm))  # Note: D@x is syntax for matrix multiplication
        problem = cvxpy.Problem(objective)
        problem.solve(solver=solver, verbose=False)
        X = np.append(X, np.array(x.value))

    if len(X) == len(data.index):
        X_series = pd.Series(X, data.index)
    else:
        X_series = pd.Series(X[:len(data.index)], data.index)

    return X_series
