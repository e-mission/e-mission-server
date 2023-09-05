import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm

def setModelConfig(metric,threshold,cutoff,clustering_way,incrementalevaluation):
    """
    TODO : tell about each param.
    pass in a test configuration to the binning algorithm.
    
    clustering_way : Part of the trip used for checking pairwise proximity.
                        Can take one of the three values:
                        
                        1. 'origin' -> using origin of the trip to check if 2 points
                                        lie within the mentioned similarity_threshold_meters
                        2. 'destination' -> using destination of the trip to check if 2 points
                                            lie within the mentioned similarity_threshold_meters
                        3. 'origin-destination' -> both origin and destination of the trip to check 
                                                if 2 points lie within the mentioned 
                                                    similarity_threshold_meters
    """        
    model_config = {
        "metric": metric,
        "similarity_threshold_meters": threshold,  # meters,
        "apply_cutoff": cutoff,
        "clustering_way": clustering_way,  
        "incremental_evaluation": incrementalevaluation
    }

    return eamtg.GreedySimilarityBinning(model_config)


def setTripConfig(trips,org,dest,trip_part,within_thr=None,label_data=None,threshold=0.001):
    """
    TODO: Tell about each
                trip_part: when mock trips are generated, coordinates of this part of 
                m trips will be within the threshold. trip_part can take one
                among the four values:
    
                1. '__' ->(None, meaning NEITHER origin nor destination of any trip will lie 
                within the mentioned threshold when trips are generated),
    
                2. 'o_' ->(origin, meaning ONLY origin of m trips will lie within the mentioned 
                threshold when trips are generated),
    
                3. '_d' ->(destination),meaning ONLY destination of m trips will lie within the 
                mentioned threshold when trips are generated)
    
                4. 'od' ->(origin and destination,meaning BOTH origin and destination of m trips
                will lie within the mentioned threshold when trips are generated)
    """
    if label_data == None:            
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

    trip =etmm.generate_mock_trips(
            user_id="joe", 
            trips=trips, 
            origin=org, 
            destination=dest,
            trip_part=trip_part,
            label_data=label_data, 
            within_threshold=within_thr, 
            threshold=threshold,  
        )
    return trip  
    