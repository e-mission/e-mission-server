import emission.core.get_database as edb
from uuid import UUID

def map_uuid_to_label(uuid, label):
    ur = edb.get_uuid_db().update_one({"uuid": uuid},
                             {"$set": {"user_email": label}})
    print(ur.raw_result)
    assert(ur.matched_count == 1)
    assert(ur.modified_count == 1)

if __name__ == '__main__':
    map_uuid_to_label(UUID("04bc05b3-4c2d-4138-bfbb-7eb45dce709a"), "ucb.sdb.iphone.3")
    map_uuid_to_label(UUID("1c8fac56-6157-4a5e-aa55-03481cad1376"), "ucb.sdb.iphone.4")
