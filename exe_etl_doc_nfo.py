import  sys
from    mongo_pkg.mng_db    import MNG_DB
from    db_pkg.db_api       import DB_API

p_pass_phrase   = sys.argv[1]
mongo_db_obj    = MNG_DB(p_pass_phrase)
db_api_obj      = DB_API(p_pass_phrase)

mongo_db_obj.etl_doc_headr_nfo()

try:
    mongo_db_obj.etl_doc_detl_nfo(1)
    mongo_db_obj.etl_doc_detl_nfo(2)
    mongo_db_obj.etl_doc_detl_nfo(3)
    mongo_db_obj.etl_doc_detl_nfo(4)
    mongo_db_obj.etl_doc_detl_nfo(5)
    mongo_db_obj.etl_doc_detl_nfo(6)
    mongo_db_obj.etl_doc_detl_nfo(7)
    mongo_db_obj.etl_doc_detl_nfo(8)
    mongo_db_obj.etl_doc_detl_nfo(9)
    mongo_db_obj.etl_doc_detl_nfo(10)
except:
    raise
else:
    db_api_obj.exe_ctas_doc_init_list()