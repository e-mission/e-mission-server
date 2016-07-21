import logging
import emission.core.get_database as edb

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    ju_email = "ju.uemura@gmail.com"
    ju_uuid = edb.get_uuid_db().find_one({'user_email': ju_email})['uuid']
    logging.debug("Found Juliana's uuid %s" % ju_uuid)
    edb.get_habitica_db().insert({'habitica_id': "e26ef1c3-d45e-464a-998c-729d107ecc42", 
        'habitica_password': u'autogenerate_me', 
        'habitica_token': '5669eb83-3e36-468d-95f4-6af15801d6cc', 
        'habitica_username': 'Juliana',
        'habitica_group_id': "488cae51-aeee-4004-9fa0-dd4219a3a77e",
        'metrics_data': {'bike_count': 0, 'last_timestamp': 1469020093, 'walk_count': 0}, 
        'user_id': ju_uuid})

    su_email = "sunil07t@gmail.com"
    su_uuid = edb.get_uuid_db().find_one({'user_email': su_email})['uuid']
    logging.debug("Found Sunil's uuid %s" % su_uuid)
    edb.get_habitica_db().insert({'habitica_id': "9ca9993a-3d84-434f-97af-3df2bd1a1def", 
        'habitica_password': u"autogenerate_me", 
        'habitica_token': "56977c97-0604-48a2-9572-59d7011b6948", 
        'habitica_username': "Sunil", 
        'habitica_group_id': "751e5f9a-bd2d-4c4c-ba81-6fb89bccdf5d",
        'metrics_data': {'bike_count': 0, 'last_timestamp': 1469020093, 'walk_count': 0}, 
        'user_id': su_uuid})

    sh_email = "shankari@berkeley.edu"
    sh_uuid = edb.get_uuid_db().find_one({'user_email': sh_email})['uuid']
    logging.debug("Found Shankari's uuid %s" % sh_uuid)
    edb.get_habitica_db().insert({'habitica_id': "e5d31351-a18c-4898-9b56-21c3dd58c834", 
        'habitica_password': u"autogenerate_me", 
        'habitica_token': "0793307f-dc24-40d2-8abe-a91fc5b685d0", 
        'habitica_username': "Shankari", 
        'habitica_group_id': "93c35a70-f70e-4d6e-ac2b-3e1c81fedf0f",
        'metrics_data': {'bike_count': 0, 'last_timestamp': 1469020093, 'walk_count': 0}, 
        'user_id': sh_uuid})
