import sys
import logging
import uuid
import pandas as pd
import arrow

import emission.core.get_database as edb

def display_checked_out_list(tz):
    all_list = list(edb.get_checkinout_db().find())
    all_df = pd.DataFrame(all_list)
    if len(all_df) == 0:
        print("No checked out bikes")
        return
    all_df["fmt_date"] = all_df.ts.apply(lambda ts: arrow.get(ts).to(tz))
    all_df.drop(["_id", "ts", "status"], axis=1, inplace=True)
    all_df
    
    print(all_df)

if __name__ == '__main__':
    tz = "UTC"
    if len(sys.argv) > 1:
        tz = sys.argv[1]
    print("timezone is %s" % tz)
    display_checked_out_list(tz)

