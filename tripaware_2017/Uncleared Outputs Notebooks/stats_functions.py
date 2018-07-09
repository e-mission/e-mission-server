import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from math import sqrt


def perm_test(labels, response_vars, stat_func, n):
    '''Labels: Series with two labels, Response_vars series in same order as labels
    stat_func is a function that takes in two series and returns a statistic, n is permutation numnber'''
    unique_label_counts = labels.value_counts()
    label_0 = unique_label_counts.index[0]
    label_1 = unique_label_counts.index[1]
    label_0_count = unique_label_counts[0]
    label_1_count = unique_label_counts[1]
    vals_0 = response_vars[labels == label_0]
    vals_1 = response_vars[labels == label_1]
    observed_stat = stat_func(vals_0, vals_1)
    sample_stats = np.array([])
    ind = labels
    for i in range(n):
        sampler = np.random.permutation(label_0_count + label_1_count)
        new_vals = response_vars.take(sampler).values
        df = pd.DataFrame({'vals': new_vals}, index=ind)
        vals_0 = df[df.index == label_0]['vals']
        vals_1 = df[df.index == label_1]['vals']
        stat = stat_func(vals_0, vals_1)
        sample_stats = np.append(sample_stats, stat)
    perm_mean = np.mean(sample_stats)
    plt.hist(sample_stats)
    plt.show()
    if observed_stat > perm_mean:
        return np.sum(sample_stats > observed_stat) / len(sample_stats)
    return np.sum(sample_stats < observed_stat) / len(sample_stats)

def mean_diff(vals_0, vals_1):
    return np.mean(vals_0) - np.mean(vals_1)

# Same as permutation testing but sampling is with replacement.
# Also don't include iteration if SD's of both groups are 0.

def bootstrap_test(labels, response_vars, stat_func, n):
    '''Labels: Series with two labels, Response_vars series in same order as labels
    stat_func is a function that takes in two series and returns a statistic, n is permutation numnber'''
    unique_label_counts = labels.value_counts()
    label_0 = unique_label_counts.index[0]
    label_1 = unique_label_counts.index[1]
    label_0_count = unique_label_counts[0]
    label_1_count = unique_label_counts[1]
    vals_0 = response_vars[labels == label_0]
    vals_1 = response_vars[labels == label_1]
    observed_stat = stat_func(vals_0, vals_1)
    sample_stats = np.array([])
    ind = labels
    for i in range(n):
        sampler = np.random.choice(np.random.permutation(label_0_count + label_1_count), label_0_count + label_1_count)
        new_vals = response_vars.take(sampler).values
        df = pd.DataFrame({'vals': new_vals}, index=ind)
        vals_0 = df[df.index == label_0]['vals']
        vals_1 = df[df.index == label_1]['vals']
        if np.std(vals_0) == 0 and np.std(vals_1) == 0:
            continue
        stat = stat_func(vals_0, vals_1)
        sample_stats = np.append(sample_stats, stat)
    perm_mean = np.mean(sample_stats)
    plt.hist(sample_stats)
    plt.show()
    if observed_stat > perm_mean:
        return np.sum(sample_stats > observed_stat) / len(sample_stats)
    return np.sum(sample_stats < observed_stat) / len(sample_stats)

def print_error_percent(p, n):
    print("p value: ", p)
    print("error percent: {0}%".format(sqrt(p * (1-p) / n) * 2 * 100))