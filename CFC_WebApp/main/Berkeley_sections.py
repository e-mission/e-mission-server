__author__ = 'Yin'
import logging
from Berkeley import get_berkeley_sections
from get_database import get_section_db


Sections=get_section_db()
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)

for section in get_berkeley_sections():
    Sections.update({'_id':section['_id']},{"$set":{'In_UCB': True}})
############################# test ##########################################
# print(get_berkeley_sections())
# for section in Sections.find({'In_UCB':True}):
#     print(section)