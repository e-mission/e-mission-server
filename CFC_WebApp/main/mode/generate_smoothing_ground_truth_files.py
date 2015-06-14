import main.mode.truth_pipeline as tp
from get_database import get_section_db
import main.gmap_display as mgp

clusterDict = tp.__read_user_clusters_text("shankari", "/Users/shankari/cluster_ground_truth/ground_truthed_clusters/manual_ground_truths")
caltrainSections = clusterDict["shankari_mtn_view_to_millbrae"]
print "Found %d sections in ground truth" % len(caltrainSections)
caltrainSectionsInDb = [s for s in caltrainSections if get_section_db().find({'_id': s}).count() == 1]
print "Found %d ground truthed sections in DB" % len(caltrainSectionsInDb)
mgp.drawSectionsSeparately(caltrainSectionsInDb, "/tmp/smoothing_ground_truth")
