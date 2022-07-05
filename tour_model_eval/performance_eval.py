# helper functions for evaluating model performance

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import itertools
import logging

import sklearn.metrics as sm
from sklearn.model_selection import KFold

# our imports
from data_wrangling import get_trip_index, get_labels
import emission.analysis.modelling.tour_model_first_only.get_users as gu
import emission.analysis.modelling.tour_model_first_only.data_preprocessing as pp

# check these
DEFAULT_MODES = [
    'walk', 'bike', 'drove_alone', 'shared_ride', 'taxi', 'bus', 'train',
    'subway', 'tramway', 'free_shuttle', 'pilot_ebike', 'not_a_trip',
    'other_mode', 'no pred', np.nan
]
DEFAULT_REPLACED = [
    'walk', 'bike', 'drove_alone', 'shared_ride', 'taxi', 'bus', 'train',
    'subway', 'tramway', 'free_shuttle', 'pilot_ebike', 'not_a_trip',
    'other_mode', 'no pred', np.nan, 'same_mode', 'no_travel'
]
DEFAULT_PURPOSES = [
    'home', 'work', 'school', 'transit_transfer', 'shopping', 'meal',
    'pick_drop', 'personal_med', 'exercise', 'entertainment', 'religious',
    'not_a_trip', 'other_purpose', 'no pred', np.nan
]

RADIUS = 500


def cross_val_predict(model, user, k=5, random_state=42, min_samples=False):
    """ Conducts k-fold cross-validation and generates predictions for the entire dataset.
    
        Concatenates the predictions from each of k folds.
        
        Returns: 
            dict containing lists of ids, predicted labels, true labels, and confidences.
        
        Args: 
            model: a model class (with fit() and predict() methods)
            user: uuid for a user
            k (int): number of folds
            random_state (int): random seed for reproducibility 
    """
    kfolds = KFold(k, random_state=random_state, shuffle=True)
    model_ = model(user)

    idx = []
    trip_idx = []
    mode_true = []
    mode_pred = []
    purpose_true = []
    purpose_pred = []
    replaced_true = []
    replaced_pred = []
    confidence = np.empty([0], dtype=int)

    trips = pp.read_data(user)

    # keeps valid trips that have user labels and are not points
    filter_trips = np.array(pp.filter_data(trips, RADIUS))

    # valid user should have >= 10 trips for further analysis and the proportion of filter_trips is >=50%
    # todo: we should update this to account for the smaller training set in each fold (80%)
    if min_samples and not gu.valid_user(filter_trips, trips):
        logging.debug(
            f"Total: {len(trips)}, labeled: {len(filter_trips)}, user {user} doesn't have enough valid trips for further analysis."
        )
        return
    elif not min_samples and len(filter_trips) < 5:
        logging.debug(
            'At least 5 valid trips are needed for cross-validation, user {} only had {}.'
            .format(user, len(filter_trips)))
        return

    for train_idx, test_idx in kfolds.split(filter_trips):
        train_trips = list(filter_trips[train_idx])
        test_trips = list(filter_trips[test_idx])
        # filter_trips needs to be an np array so that we can do the easy indexing,
        # but train_trips needs to be a list because one of the pipeline functions called in
        # first_round_cluster.fit() needs a list
        # In the future we should convert everything to dataframes because those are
        # nicest to work with.

        idx = np.append(idx, test_idx)
        trip_idx = np.append(trip_idx, get_trip_index(test_trips))

        next_mode_true, next_purpose_true, next_replaced_true = get_labels(
            test_trips)
        mode_true += next_mode_true
        purpose_true += next_purpose_true
        replaced_true += next_replaced_true

        model_.fit(train_trips)
        next_mode_pred, next_purpose_pred, next_replaced_pred, next_conf = model_.predict(
            test_trips)
        mode_pred += next_mode_pred
        purpose_pred += next_purpose_pred
        replaced_pred += next_replaced_pred
        confidence = np.append(confidence, next_conf)

    return {
        'idx': idx,
        'trip_idx': trip_idx,
        'mode_true': mode_true,
        'purpose_true': purpose_true,
        'replaced_true': replaced_true,
        'mode_pred': mode_pred,
        'purpose_pred': purpose_pred,
        'replaced_pred': replaced_pred,
        'confidence': confidence
    }


def get_metrics(trip_df, label_type, keep_nopred=True, ignore_custom=False):
    """ Args:
            trip_df: DataFrame with true labels and predicted labels. Any missing labels should be marked with np.nan
                Should have the following columns: 'mode_true', 'purpose_true', 'replaced_true', 
                'mode_pred', 'purpose_pred', 'replaced_pred', 'top_pred'
            label_type (str): 'mode', 'purpose', 'replaced', or 'tuple'
            keep_nopred (bool): whether or not to keep trips without a predicted label
            ignore_custom (bool): whether or not to remove custom labels
    """
    if label_type not in ['mode', 'purpose', 'replaced', 'tuple']:
        raise Exception('incorrect label_type')

    labels = np.sort(trip_df[label_type + '_true'].dropna().unique())

    # do a bunch of data filtering to keep only the desired trips

    # only keep trips that have a user input, and filter out
    # alternative predictions if there were multiple predicted labels
    labeled_predicted_df = trip_df[trip_df[label_type + '_true'].notnull() & (
        trip_df['top_pred'] | (trip_df[label_type + '_pred'].isnull()))]

    if keep_nopred:
        labels = np.append(labels, ['no pred'])
    else:
        no_pred_trips = labeled_predicted_df[labeled_predicted_df[
            label_type + '_pred'].isnull()]

        #         labeled_predicted_df = trip_df[trip_df[label_type + '_true'].notnull() & (trip_df['top_pred'] )]

        print(
            '{} non-predicted trips ignored out of {} total trips with user-labeled {}\n'
            .format(len(no_pred_trips), len(labeled_predicted_df), label_type))

        labeled_predicted_df = labeled_predicted_df[
            ~labeled_predicted_df[label_type + '_pred'].isnull()]

    if ignore_custom and label_type == "mode":
        print('excluded labels:',
              [l for l in labels if l not in DEFAULT_MODES])
        print()

        custom_trips = labeled_predicted_df[
            ~labeled_predicted_df[label_type + '_pred'].isin(DEFAULT_MODES)]
        print(
            '{} custom trips ignored out of {} total trips with user-labeled {}'
            .format(len(custom_trips), len(labeled_predicted_df), label_type))
        print()

        labels = [l for l in labels if l in DEFAULT_MODES]
        labeled_predicted_df = labeled_predicted_df[labeled_predicted_df[
            label_type + '_true'].isin(DEFAULT_MODES)]
        labeled_predicted_df = labeled_predicted_df[labeled_predicted_df[
            label_type + '_pred'].isin(DEFAULT_MODES)]

    elif ignore_custom and label_type == "replaced":
        print('excluded labels:',
              [l for l in labels if l not in DEFAULT_REPLACED])
        print()

        custom_trips = labeled_predicted_df[
            ~labeled_predicted_df[label_type + '_pred'].isin(DEFAULT_REPLACED)]
        print(
            '{} custom trips ignored out of {} total trips with user-labeled {}'
            .format(len(custom_trips), len(labeled_predicted_df), label_type))
        print()

        labels = [l for l in labels if l in DEFAULT_REPLACED]
        labeled_predicted_df = labeled_predicted_df[labeled_predicted_df[
            label_type + '_true'].isin(DEFAULT_REPLACED)]
        labeled_predicted_df = labeled_predicted_df[labeled_predicted_df[
            label_type + '_pred'].isin(DEFAULT_REPLACED)]

    elif ignore_custom and label_type == "purpose":
        print('excluded labels:',
              [l for l in labels if l not in DEFAULT_PURPOSES])
        print()

        custom_trips = labeled_predicted_df[
            ~labeled_predicted_df[label_type + '_pred'].isin(DEFAULT_PURPOSES)]
        print(
            '{} custom trips ignored out of {} total trips with user-labeled {}'
            .format(len(custom_trips), len(labeled_predicted_df), label_type))
        print()

        labels = [l for l in labels if l in DEFAULT_PURPOSES]
        labeled_predicted_df = labeled_predicted_df[labeled_predicted_df[
            label_type + '_true'].isin(DEFAULT_PURPOSES)]
        labeled_predicted_df = labeled_predicted_df[labeled_predicted_df[
            label_type + '_pred'].isin(DEFAULT_PURPOSES)]

    # ok now done with filtering, we can finally extract the labels
    label_true = labeled_predicted_df[label_type + '_true'].astype(str)
    label_pred = labeled_predicted_df[label_type +
                                      '_pred'].fillna('no pred').astype(str)

    cm = sm.confusion_matrix(label_true, label_pred, labels=labels)
    mcm = sm.multilabel_confusion_matrix(label_true, label_pred, labels=labels)

    class_precision, class_recall, class_f_score, class_support = sm.precision_recall_fscore_support(
        label_true, label_pred, labels=labels)
    #     class_precision = cm.diagonal()/cm.sum(axis=0)
    #     class_recall = cm.diagonal()/cm.sum(axis=1)
    #     class_f_score = 2 * class_precision * class_recall / (class_precision + class_recall)
    class_accuracy = np.array([cm.diagonal().sum() / cm.sum() for cm in mcm])

    precision = sm.precision_score(label_true, label_pred, average='micro')
    recall = sm.recall_score(label_true, label_pred, average='micro')
    f_score = sm.f1_score(label_true, label_pred, average='micro')
    accuracy = sm.accuracy_score(label_true, label_pred)

    # TODO: it would also be nice to return a count of labeled/unlabeled trips,
    # predicted/non-predicted trips, etc
    return {
        'labels': labels,
        'cm': cm,
        'mcm': mcm,
        'class_precision': class_precision,
        'class_recall': class_recall,
        'class_f_score': class_f_score,
        'class_accuracy': class_accuracy,
        'precision': precision,
        'recall': recall,
        'f_score': f_score,
        'accuracy': accuracy
    }


def print_metrics(trip_df, label_type, keep_nopred=True, ignore_custom=False):
    """ prints results with nice formatting and plots the confusion matrix.
    
        label_type = 'mode', 'purpose', 'replaced', or 'tuple'
    """
    results = get_metrics(trip_df, label_type, keep_nopred, ignore_custom)

    format_header = "{:>15}{:^11}{:^8}{:^8}{:^15}"
    format_row = "{:>15.15}{:^11.2f}{:^8.2f}{:^8.2f}{:^15.2f}"

    print(
        format_header.format('Label', 'Precision', 'Recall', 'F-Score',
                             'Class Accuracy'))
    for i in range(len(results['labels'])):
        print(
            format_row.format(results['labels'][i],
                              round(results['class_precision'][i], 3),
                              round(results['class_recall'][i], 3),
                              round(results['class_f_score'][i], 3),
                              round(results['class_accuracy'][i], 3)))

    print()
    print('micro precision\t ', np.round(results['precision'], 2))
    print('micro recall\t ', np.round(results['recall'], 2))
    print('micro f_score\t ', np.round(results['f_score'], 2))
    print('overall accuracy ', np.round(results['accuracy'], 2))

    title = 'Confusion matrix for ' + label_type
    if keep_nopred:
        title += '\nkeep non-predicted trips'
    else:
        title += '\nexclude non-predicted trips'
    if ignore_custom:
        title += '; ignore custom labels'
    else:
        title += '; keep custom labels'

    plot_cm(results['cm'], results['labels'], title=title)


def plot_cm(cm, classes, ax=None, title='Confusion matrix'):
    """ Plots a confusion matrix with colorbar.
      
        cm: confusion matrix
        classes: list of labels
    """
    if ax is None:
        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    mappable = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
    ax.set_title(title)
    plt.colorbar(mappable, ax=ax)
    tick_marks = np.arange(len(classes))
    ax.set_xticks(np.arange(len(classes)))
    ax.set_yticks(np.arange(len(classes)))
    ax.set_xticklabels(classes, rotation=80)
    ax.set_yticklabels(classes)

    color_thresh = cm.max() / 2
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        ax.text(j,
                i,
                cm[i, j],
                horizontalalignment='center',
                color='white' if cm[i, j] > color_thresh else 'black')

    # plt.tight_layout()
    ax.set_ylabel('True label')
    ax.set_xlabel('Predicted label')


def plot_mcm(mcm,
             classes,
             normalize=False,
             title='Confusion matrix',
             cmap=plt.cm.Blues,
             figsize=(10, 10)):
    """ Plots the multilabel confusion matrices. 
        (Plots are pretty ugly but reformating is not a priority right now.)
    """
    fig = plt.figure(figsize=figsize)
    for i in range(len(classes)):
        cm = mcm[i]
        ax = fig.add_subplot(2, len(classes) // 2 + len(classes) % 2, i + 1)
        plot_cm(cm, ['not ' + classes[i], classes[i]], ax)


#         plt.tight_layout()