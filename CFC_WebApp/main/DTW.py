__author__ = 'Mogeng'
from tripManager import calDistance
import numpy as np

class Dtw(object):
    def __init__(self, seq1, seq2, distance_func=None):
        '''
        seq1, seq2 are two lists,
        distance_func is a function for calculating
        the local distance between two elements.
        '''

        size1=len(seq1)
        # print(size1)
        if size1<=100:
        # print(size)
            self._seq1 = seq1
        else:
            indexes=np.arange(0,size1,int(math.ceil(size1/100)))
            # print(len(indexes))
            self._seq1 = []
            for i in indexes:
                self._seq1.append(seq1[i])
            self._seq1.append(seq1[-1])
        size2=len(seq2)
        # print(size2)
        if size2<=100:
        # print(size)
            self._seq2 = seq2
        else:
            indexes=np.arange(0,size2,int(math.ceil(size2/100)))
            # print(len(indexes))
            self._seq2 = []
            for i in indexes:
                self._seq2.append(seq2[i])
            self._seq2.append(seq2[-1])

        self._distance_func = distance_func if distance_func else lambda: 0
        self._map = {(-1, -1): 0.0}
        self._distance_matrix = {}
        self._path = []

    def get_distance(self, i1, i2):
        ret = self._distance_matrix.get((i1, i2))
        if not ret:
            ret = self._distance_func(self._seq1[i1], self._seq2[i2])
            self._distance_matrix[(i1, i2)] = ret
        return ret

    def calculate_backward(self, i1, i2):
        '''
        Calculate the dtw distance between
        seq1[:i1 + 1] and seq2[:i2 + 1]
        '''
        if self._map.get((i1, i2)) is not None:
            return self._map[(i1, i2)]

        if i1 == -1 or i2 == -1:
            self._map[(i1, i2)] = float('inf')
            return float('inf')

        min_i1, min_i2 = min((i1 - 1, i2), (i1, i2 - 1), (i1 - 1, i2 - 1),
                             key=lambda x: self.calculate_backward(*x))

        self._map[(i1, i2)] = self.get_distance(i1, i2) + \
            self.calculate_backward(min_i1, min_i2)

        return self._map[(i1, i2)]

    def get_path(self):
        '''
        Calculate the path mapping.
        Must be called after calculate()
        '''
        i1, i2 = (len(self._seq1) - 1, len(self._seq2) - 1)
        while (i1, i2) != (-1, -1):
            self._path.append((i1, i2))
            min_i1, min_i2 = min((i1 - 1, i2), (i1, i2 - 1), (i1 - 1, i2 - 1),
                                 key=lambda x: self._map[x[0], x[1]])
            i1, i2 = min_i1, min_i2
        return self._path

    def calculate(self):
        return self.calculate_backward(len(self._seq1) - 1,
                                       len(self._seq2) - 1)

    def calculate_distance(self):
        return self.calculate()/len(self.get_path())

import math

def dynamicTimeWarp(seqA, seqB, d = calDistance):
    # create the cost matrix
    numRows, numCols = len(seqA), len(seqB)
    cost = [[0 for _ in range(numCols)] for _ in range(numRows)]

    # initialize the first row and column
    cost[0][0] = d(seqA[0], seqB[0])
    for i in xrange(1, numRows):
        cost[i][0] = cost[i-1][0] + d(seqA[i], seqB[0])

    for j in xrange(1, numCols):
        cost[0][j] = cost[0][j-1] + d(seqA[0], seqB[j])

    # fill in the rest of the matrix
    for i in xrange(1, numRows):
        for j in xrange(1, numCols):
            choices = cost[i-1][j], cost[i][j-1], cost[i-1][j-1]
            cost[i][j] = min(choices) + d(seqA[i], seqB[j])

    # for row in cost:
    #    for entry in row:
    #       print "%03d" % entry,
    #    print ""
    return cost[-1][-1]

class DtwSym(object):
    def __init__(self, seq1, seq2, distance_func=None):
        '''
        seq1, seq2 are two lists,
        distance_func is a function for calculating
        the local distance between two elements.
        '''
        self._seq1 = seq1
        self._seq2 = seq2
        self._distance_func = distance_func if distance_func else lambda: 0
        self._map = {(-1, -1): 0.0}
        self._distance_matrix = {}
        self._path = []

    def get_distance(self, i1, i2):
        ret = self._distance_matrix.get((i1, i2))
        if not ret:
            ret = self._distance_func(self._seq1[i1], self._seq2[i2])
            self._distance_matrix[(i1, i2)] = ret
        return ret

    def calculate_backward(self, i1, i2):
        '''
        Calculate the dtw distance between
        seq1[:i1 + 1] and seq2[:i2 + 1]
        '''
        if self._map.get((i1, i2)) is not None:
            return self._map[(i1, i2)]

        if i1 == -1 or i2 == -1:
            self._map[(i1, i2)] = float('inf')
            return float('inf')

        min_i1, min_i2 = min((i1 - 1, i2), (i1, i2 - 1), (i1 - 1, i2 - 1),
                             key=lambda x: self.calculate_backward(*x))

        self._map[(i1, i2)] = self.get_distance(i1, i2) * (i1+i2-min_i1-min_i2) + \
            self.calculate_backward(min_i1, min_i2)

        return self._map[(i1, i2)]

    def get_path(self):
        '''
        Calculate the path mapping.
        Must be called after calculate()
        '''
        i1, i2 = (len(self._seq1) - 1, len(self._seq2) - 1)
        while (i1, i2) != (-1, -1):
            self._path.append((i1, i2))
            min_i1, min_i2 = min((i1 - 1, i2), (i1, i2 - 1), (i1 - 1, i2 - 1),
                                 key=lambda x: self._map[x[0], x[1]])
            i1, i2 = min_i1, min_i2
        return self._path

    def calculate(self):
        return self.calculate_backward(len(self._seq1) - 1,
                                       len(self._seq2) - 1)

    def calculate_distance(self):
        return self.calculate()/(len(self._seq1)+len(self._seq2))

class DtwAsym(object):
    def __init__(self, seq1, seq2, distance_func=None):
        '''
        seq1 is the one that we are interested to match with seq2
        '''
        self._seq1 = seq1
        self._seq2 = seq2
        self._distance_func = distance_func if distance_func else lambda: 0
        self._map = {(-1, -1): 0.0}
        self._distance_matrix = {}
        self._path = []

    def get_distance(self, i1, i2):
        ret = self._distance_matrix.get((i1, i2))
        if not ret:
            ret = self._distance_func(self._seq1[i1], self._seq2[i2])
            self._distance_matrix[(i1, i2)] = ret
        return ret

    def calculate_backward(self, i1, i2):
        '''
        Calculate the dtw distance between
        seq1[:i1 + 1] and seq2[:i2 + 1]
        '''
        if self._map.get((i1, i2)) is not None:
            return self._map[(i1, i2)]

        if i1 == -1 or i2 == -1:
            self._map[(i1, i2)] = float('inf')
            return float('inf')

        min_i1, min_i2 = min((i1 - 1, i2), (i1, i2 - 1), (i1 - 1, i2 - 1),
                             key=lambda x: self.calculate_backward(*x))

        self._map[(i1, i2)] = self.get_distance(i1, i2) * (i1-min_i1) + \
            self.calculate_backward(min_i1, min_i2)

        return self._map[(i1, i2)]

    def get_path(self):
        '''
        Calculate the path mapping.
        Must be called after calculate()
        '''
        i1, i2 = (len(self._seq1) - 1, len(self._seq2) - 1)
        while (i1, i2) != (-1, -1):
            self._path.append((i1, i2))
            min_i1, min_i2 = min((i1 - 1, i2), (i1, i2 - 1), (i1 - 1, i2 - 1),
                                 key=lambda x: self._map[x[0], x[1]])
            i1, i2 = min_i1, min_i2
        return self._path

    def calculate(self):
        return self.calculate_backward(len(self._seq1) - 1,
                                       len(self._seq2) - 1)

    def calculate_distance(self):
        return self.calculate()/len(self._seq1)
