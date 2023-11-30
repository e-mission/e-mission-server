import logging

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.cluster_pipeline as pipeline
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
import emission.storage.decorations.analysis_timeseries_queries as esda
import pandas as pd
from numpy import *

# Spanish words to English
span_eng_dict = {'revisado_bike':'test ride with bike','placas_de carro':'car plates','aseguranza':'insurance',
 'iglesia':'church','curso':'course','mi_hija recién aliviada':'my daughter just had a new baby',
 'servicio_comunitario':'community service','pago_de aseguranza':'insurance payment',
 'grupo_comunitario':'community group','caminata_comunitaria':'community walk'}

# Convert purpose
map_pur_dict = {'course':'school','work_- lunch break':'lunch_break','on_the way home':'home',
               'insurance_payment':'insurance'}

# precision_bins takes five parameters
# - all_bins_preci: the list that collects precision of each bin, should pass in an empty list
# - sp2en=None means no need to translate language
#   sp2en='True' will use span_eng_dict to change Spanish to English
#
# - cvt_purpose=None means no need to convert purposes
#   cvt_purpose='True' will use map_pur_dict to convert purposes
#   using this parameter should also set sp2en='True'
def precision_bins (all_bins_preci,bins,non_empty_trips,sp2en=None,cvt_purpose=None):
    for bin in bins:
        bin_user_input = (non_empty_trips[i].data["user_input"] for i in bin if
                          non_empty_trips[i].data["user_input"] != {})
        bin_df = pd.DataFrame(data=bin_user_input)
        if sp2en == 'True':
            bin_df = bin_df.replace(span_eng_dict)
        if cvt_purpose == 'True':
            bin_df = bin_df.replace(map_pur_dict)
        duplic_trips = bin_df[bin_df.duplicated(keep=False)]

        # for bin that doesn't have duplicate trips, assign 0 as precision
        if duplic_trips.empty and len(bin_df) > 1:
            all_bins_preci.append(0)
        # for bin only has one trip, assign 1.0 as precision
        elif len(bin_df) == 1:
            all_bins_preci.append(1.0)
        else:
            duplic = duplic_trips.groupby(duplic_trips.columns.tolist()).apply(lambda x: tuple(x.index)).tolist()
            max_duplic = max(duplic, key=lambda i: len(i))
            precision = round(len(max_duplic) / len(bin), 2)
            all_bins_preci.append(precision)
    return all_bins_preci


# precision_all_users takes four parameters
# - all_users: pass in all participants' data
# - sp2en: default None, no need to change language
# - cvt_purpose: default None, no need to convert purpose
def precision_bin_all_users(all_users,radius,sp2en=None,cvt_purpose=None):
    all_users_preci = []
    for i in range(len(all_users)):
        user = all_users[i]
        trips = pipeline.read_data(uuid=user, key=esda.CONFIRMED_TRIP_KEY)
        all_bins_preci = []
        non_empty_trips = [t for t in trips if t["data"]["user_input"] != {}]
        if non_empty_trips != {}:
            sim = similarity.similarity(non_empty_trips, radius)
            if sim.data:
                sim.bin_data()
                all_bins_preci = precision_bins(all_bins_preci, sim.bins, non_empty_trips, sp2en, cvt_purpose)
        all_users_preci.append(round(mean(all_bins_preci), 2))
    return all_users_preci
