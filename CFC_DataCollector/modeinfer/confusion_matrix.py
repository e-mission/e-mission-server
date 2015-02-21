from sklearn import cross_validation, metrics
import matplotlib as mpl
from matplotlib import cm
import numpy as np
import matplotlib.pyplot as plt


def printConfusionMatrix(algo, X, y, title):
    skf = cross_validation.StratifiedKFold(y, 5)
    nClasses = np.count_nonzero(np.unique(y))
    print "nClasses = %s" % nClasses
    sumPCM = np.zeros([nClasses, nClasses])
    for train, test in skf:
        X_train, X_test, y_train, y_test = X[train], X[test], y[train], y[test]
        print "Number of distinct classes in training set = %s, test set = %s" % (np.unique(y[train]), np.unique(y[test]))
        y_pred = algo.fit(X_train, y_train).predict(X_test)
        # This has the raw number of entries (e.g. [610  12   1   0  32   1])
        # Since the total number of entries for each mode is different, we want to convert this to a percentage
        cmraw = metrics.confusion_matrix(y_test, y_pred)
        # We do that by summing up the entries for each mode (e.g. 656)
        sumArr = np.sum(cmraw, axis=1)
        # and repeating it across the row (e.g. [656 656 656 656 656 656])
        repeatedSumArr = np.repeat(sumArr, cmraw.shape[1]).reshape(cmraw.shape)
        # And dividing the raw numbers by the sums to get percentages (e.g [92.98 1.82 0 4.87 0.15])
        sumPCM = np.add(sumPCM, np.divide(cmraw.astype(float), repeatedSumArr))
    
    finalPCM = sumPCM / 5
    logFinalPCM = np.log(finalPCM + 1)
    np.set_printoptions(precision=0, suppress=True)
    # np.set_printoptions(precision=4, suppress=False)
    print(finalPCM * 100)

    oldSize = mpl.rcParams['font.size']
    mpl.rcParams['font.size'] = 16
    (fig, ax) = plt.subplots()
    # First element is "" because of http://stackoverflow.com/questions/3529666/matplotlib-matshow-labels
    ax.set_xticklabels(["","walk", "", "bus", "", "car", ""])
    ax.set_yticklabels(["","walk", "cycle", "bus", "train", "car", "air"])
    cax = ax.matshow(logFinalPCM, cmap=cm.gray)
    ax.set_title(title, color='green', weight='bold', size=16, y=1.1)
    
    fig.colorbar(cax)
    ax.set_ylabel('True label', size="large")
    ax.set_xlabel('Predicted label', size="large")
    fig.tight_layout()
    plt.show()
    return (finalPCM, fig)
