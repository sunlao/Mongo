import  sys
import  datetime
from    mongo_pkg.mng_db        import MNG_DB
from    mongo_pkg.mng_doc_dict  import MNG_Doc_Dict

p_pass_phrase   = sys.argv[1]

mongo_mgt_obj   = MNG_DB(p_pass_phrase)
mongo_dict_obj  = MNG_Doc_Dict()

v_last_check_dt = datetime.datetime.strptime('2015-04-10 22:38.00' , '%Y-%m-%d %H:%M.%S')



v_db_list = mongo_mgt_obj.get_db_list()

for v_db_nm in v_db_list:
    print "Database name := %s" %(v_db_nm)

    v_collection_list = mongo_mgt_obj.get_collection_list(v_db_nm)
    for v_collection_nm in v_collection_list:
        print "**Collection Name :={0:s}".format(v_collection_nm)

        # v_doc_all_obj = mongo_mgt_obj.get_doc_all_obj(v_db_nm,v_collection_nm)
        # for v_doc_all_dict in v_doc_all_obj:
        #     v_doc_all_id        = mongo_mgt_obj.get_doc_id(v_doc_all_dict)
        #     v_doc_all_crt_dt    = mongo_mgt_obj.get_doc_crt_dt(v_doc_all_id)
        #
        #     print "****doc all id := %s was created on %s" %(v_doc_all_id,v_doc_all_crt_dt)

        v_doc_dt_obj = mongo_mgt_obj.get_doc_obj_by_dt(v_db_nm,v_collection_nm,v_last_check_dt)
        for v_doc_dt_dict in v_doc_dt_obj:
            v_doc_dt_id        = v_doc_dt_dict['_id']
            v_doc_dt_crt_dt    = mongo_dict_obj.get_crt_dt(v_doc_dt_id)

            print "**** doc dt id := %s was created on %s" %(v_doc_dt_id, v_doc_dt_crt_dt)

            for v_key in v_doc_dt_dict:
                if v_key != '_id':
                    v_typ = type(v_doc_dt_dict[v_key]).__name__
                    if  v_typ == 'list':
                        print "******List name := %s" %(v_key)
                        v_list = v_doc_dt_dict[v_key]
                        for v_value in v_list:
                            print "********List value is := %s" %(v_value)
                    else:
                        print ("******For Key := '%s' the Value := '%s' which has as type := %s") %(v_key,v_doc_dt_dict[v_key],v_typ)

        # v_doc_id_dict = mongo_mgt_obj.get_doc_dict_by_id(v_db_nm,v_collection_nm,v_doc_dt_id)
        #
        # for v_key in v_doc_id_dict:
        #      if v_key != '_id':
        #         v_typ = type(v_doc_id_dict[v_key]).__name__
        #         if  v_typ == 'list':
        #             print "******List name := %s for doc id := %s" %(v_key,v_doc_dt_id)
        #             v_list = v_doc_id_dict[v_key]
        #             for v_value in v_list:
        #                 print "********List value is := %s" %(v_value)
        #         else:
        #             print "******For Key := '%s' the Value := '%s' which has as type := %s for doc id := %s" %(v_key,v_doc_id_dict[v_key],v_typ,v_doc_dt_id)