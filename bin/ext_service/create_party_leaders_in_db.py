import logging
import emission.core.get_database as edb
import emission.net.ext_service.habitica.create_party_leaders_script as lead

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    lead.create_party_leaders()
