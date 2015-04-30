"""
Cluster evaluation pipeline

Initial implementation details can be found on the google doc
"""

import os, sys, random
sys.path.append("%s/../" % os.getcwd())

metrics = ('dtw', 'lcs')
methods = ('kmedoid')

def __extract_trip_features():
    pass

def __cluster_partitions(metric='dtw', method='kmedoid'):
    pass

def __evaluate_clusters():
    pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Cluster Evaluation')
    parser.add_argument('stage', metavar='S', type=str, choices=['extract', 'cluster', 'evaluate', 'run'], 
                        help='Select a pipeline stage, or all')
    parser.add_argument('-f', '--force', dest='force', action='store_const',
                        const=True, default=False,
                        help='Force overwrite of stored data')

    args = parser.parse_args()
    stage = args.stage

    if stage == 'extract':
        print('Extracting features from trips')
        __extract_trip_features()
    elif stage == 'cluster':
        print('Clustering partitioned trips')
        __cluster_partitions()
    elif stage == 'evaluate':
        print('Evaluating clusters')
        __evaluate_clusters()
    elif stage == 'run':
        # Initial implementation only iterates over
        # distance partitions        
        __extract_trip_features()
        __cluster_partitions()
        __evaluate_clusters()
