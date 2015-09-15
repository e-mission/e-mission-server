import logging
import emission.net.usercache.formatters.android.motion_activity as fam

def format(entry):
    entry.metadata.key = "background/motion_activity"
    return fam.format(entry)
