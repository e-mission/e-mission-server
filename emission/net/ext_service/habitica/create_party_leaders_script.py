import logging
import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy
import emission.core.wrapper.user as ecwu


def create_party_leaders():
    ju_email = "ju.uemura@gmail.com"
    ecwu.User.register(ju_email)
    ju_uuid = edb.get_uuid_db().find_one({'user_email': ju_email})['uuid']
    logging.debug("Found Juliana's uuid %s" % ju_uuid)
    proxy.habiticaRegister("Juliana", ju_email, "autogenerate_me", ju_uuid)

    su_email = "sunil07t@gmail.com"
    ecwu.User.register(su_email)
    su_uuid = edb.get_uuid_db().find_one({'user_email': su_email})['uuid']
    logging.debug("Found Sunil's uuid %s" % su_uuid)
    proxy.habiticaRegister("Sunil", su_email, "autogenerate_me", su_uuid)

    sh_email = "shankari@berkeley.edu"
    ecwu.User.register(sh_email)
    sh_uuid = edb.get_uuid_db().find_one({'user_email': sh_email})['uuid']
    logging.debug("Found Shankari's uuid %s" % sh_uuid)
    proxy.habiticaRegister("Shankari", sh_email, "autogenerate_me", sh_uuid)