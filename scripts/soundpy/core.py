import numpy as np
import pandas as pd
import scipy
from numpy.random import default_rng

rng = default_rng(seed=1)
from sklearn.utils import resample
import matplotlib.pyplot as plt
from scipy.stats import ksone


def check_unary_point_wise(x, eps, fn_test, n_samples=10):
    """apply a function to every element in the series using bootstrap_single and return an array of the ratio of successes"""
    results = np.zeros(len(x))

    # create samples of x by repeating original values and adding gaussian noise with std=eps
    x_samples = np.zeros((n_samples, len(x)))
    for i in range(n_samples):
        x_samples[i] = x + np.random.normal(0, eps, len(x))

    # apply fn_test to every element in x_samples and store count of trues in results
    for i in range(len(x)):
        results[i] = np.sum(fn_test(x_samples[:, i])) / n_samples

    return results


def bootstrap_single(
    x,
    eps,
    fn_ts,
    n_bootstrap_repetitions=20,
    alpha=0.05,
    min_success_rate_excpected=0.5,
):
    results = np.zeros(n_bootstrap_repetitions)

    for i in range(n_bootstrap_repetitions):
        if eps is not None:
            rnd_error = np.random.normal(0, eps / 2, len(x))
        else:
            rnd_error = np.zeros_like(x)

        d1_boot = resample(x + rnd_error, replace=True, n_samples=len(x))
        results[i] = fn_ts(d1_boot)

    # get ratio of successes
    success_rate = np.sum(results) / len(results)

    # confidence_interval = np.percentile(test_statistic, [100 * (alpha / 2), 100 * (1 - alpha / 2)])

    # get ratio of ts larger than min_success_rate_excpected
    # success_rate = np.sum(test_statistic > min_success_rate_excpected) / len(test_statistic)

    # return confidence_interval

    return success_rate


def check_unary_point_wise(x, eps, fn_test):
    """apply a function to every element in the series using bootstrap_single and return an array of the ratio of successes"""
    results = np.zeros(len(x))

    # x and eps are 1d numpy arrays of same length. iterate over pairs of them and apply bootstrap_single(x[i], eps, fn_test)
    for i in range(len(x)):
        results[i] = bootstrap_single(x[i], eps[i], fn_test)

    return results
