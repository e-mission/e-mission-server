import emission.core.get_database as edb
from uuid import UUID

def map_uuid_to_label(uuid, label):
    ur = edb.get_uuid_db().update_one({"uuid": uuid},
                             {"$set": {"user_email": label}})
    print(ur.raw_result)
    assert(ur.matched_count == 1)
    assert(ur.modified_count == 1)

if __name__ == '__main__':
    map_uuid_to_label(UUID("e471711e-bd14-3dbe-80b6-9c7d92ecc296"), "ucb.sdb.android.1")
    map_uuid_to_label(UUID("fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7"), "ucb.sdb.android.2")
    map_uuid_to_label(UUID("86842c35-da28-32ed-a90e-2da6663c5c73"), "ucb.sdb.android.3")
    map_uuid_to_label(UUID("3bc0f91f-7660-34a2-b005-5c399598a369"), "ucb.sdb.android.4")

    map_uuid_to_label(UUID("079e0f1a-c440-3d7c-b0e7-de160f748e35"), "ucb.sdb.iphone.1")
    map_uuid_to_label(UUID("c76a0487-7e5a-3b17-a449-47be666b36f6"), "ucb.sdb.iphone.2")
    map_uuid_to_label(UUID("c528bcd2-a88b-3e82-be62-ef4f2396967a"), "ucb.sdb.iphone.3")
    map_uuid_to_label(UUID("95e70727-a04e-3e33-b7fe-34ab19194f8b"), "ucb.sdb.iphone.4")

    map_uuid_to_label(UUID("70968068-dba5-406c-8e26-09b548da0e4b"), "nexus7itu01")
    map_uuid_to_label(UUID("6561431f-d4c1-4e0f-9489-ab1190341fb7"), "nexus7itu02")

    map_uuid_to_label(UUID("92cf5840-af59-400c-ab72-bab3dcdf7818"), "motoeitu01")
    map_uuid_to_label(UUID("93e8a1cc-321f-4fa9-8c3c-46928668e45d"), "motoeitu02")
