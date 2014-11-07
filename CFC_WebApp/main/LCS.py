from __future__ import division
from tripManager import calDistance
def lcs(a, b,radiusBound):
    lengths = [[0 for j in range(len(b)+1)] for i in range(len(a)+1)]
    # row 0 and column 0 are initialized to 0 already
    for i, x in enumerate(a):
        for j, y in enumerate(b):
            if calDistance(x,y)<=radiusBound:
                lengths[i+1][j+1] = lengths[i][j] + 1
            else:
                lengths[i+1][j+1] = \
                    max(lengths[i+1][j], lengths[i][j+1])
    # read the substring out from the matrix
    # result = []
    # x, y = len(a), len(b)
    # while x != 0 and y != 0:
    #     print('aa')
    #     if lengths[x][y] == lengths[x-1][y]:
    #         x -= 1
    #     elif lengths[x][y] == lengths[x][y-1]:
    #         y -= 1
    #     else:
    #         assert calDistance(a[x-1],b[y-1])<=radiusBound
    #         result.append(a[x-1])
    #         x -= 1
    #         y -= 1
    # print(result)
    return lengths[-1][-1]

def lcsScore(route1, route2,radiusBound):
    return 1-lcs(route1, route2,radiusBound)/min(len(route1),len(route2))