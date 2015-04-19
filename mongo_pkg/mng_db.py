from    bson.objectid       import ObjectId
import  datetime

class MNG_DB(object):

    def __init__(self,p_pass_phrase):
        from util_pkg.config            import Config
        from pymongo                    import MongoClient
        from mongo_pkg.mng_sys_dict  import MNG_Sys_Dict
        from db_pkg.db_api              import DB_API
        from mongo_pkg.mng_doc_kvp      import MNG_Doc_KVP

        config_obj                  = Config()
        v_mongo_url                 = config_obj.get_value('mongo_url')

        self.client_obj             = MongoClient(v_mongo_url)
        self.mongo_data_dict_obj    = MNG_Sys_Dict(p_pass_phrase)
        self.db_api_obj             = DB_API(p_pass_phrase)
        self.mng_doc_kvp_obj        = MNG_Doc_KVP(p_pass_phrase)

    def get_db_list(self):
        v_db_list = self.client_obj.database_names()
        v_db_list.remove('local')

        return v_db_list

    def get_collection_list(self, p_db_nm):
        db_obj = self.client_obj[p_db_nm]

        v_collection_list   = db_obj.collection_names()
        for v_collection_nm in v_collection_list:
            if "system." in v_collection_nm:
                v_collection_list.remove(v_collection_nm)

        return v_collection_list

    def get_doc_all_obj(self, p_db_nm, p_collection_nm):
        db_obj              = self.client_obj[p_db_nm]
        v_doc_obj           = db_obj[p_collection_nm].find()

        return v_doc_obj

    def get_doc_obj_by_dt(self, p_db_nm, p_collection_nm, p_query_dt):
        v_query_dt_str      = p_query_dt.strftime('%Y-%m-%d %H:%M.%S')
        v_query_dt_utc      = datetime.datetime.strptime(v_query_dt_str, '%Y-%m-%d %H:%M.%S')
        v_query_obj_id      = ObjectId.from_datetime(v_query_dt_utc)
        db_obj              = self.client_obj[p_db_nm]
        v_doc_obj           = db_obj[p_collection_nm].find({"_id": {"$gte": v_query_obj_id}})
        return v_doc_obj

    def get_doc_dict_by_id(self,p_db_nm, p_collection_nm, p_doc_id):
        db_obj              = self.client_obj[p_db_nm]
        v_doc_obj           = db_obj[p_collection_nm].find({"_id":ObjectId(p_doc_id)})
        v_doc_dict          = v_doc_obj[0]

        return v_doc_dict

    def put_doc(self,p_db_nm, p_collection_nm,p_dict):
        db_obj      = self.client_obj[p_db_nm]
        v_res_id    = db_obj[p_collection_nm].insert(p_dict)

        return v_res_id

    def etl_doc_headr_nfo(self):
        import  datetime
        v_load_date         = datetime.datetime.utcnow()
        v_sys_data_dict     = self.mongo_data_dict_obj.get_dict()
        v_data_base_nm_dict = v_sys_data_dict['DATA_BASE_NM_DICT']
        v_db_list           = self.get_db_list()

        for v_db_nm in v_db_list:
            v_last_load_dt      = self.db_api_obj.get_last_load_dt(v_db_nm)
            v_db_data_pk        = self.db_api_obj.get_rep_attr_data_pk(
                                    p_attr_fk   = v_data_base_nm_dict['REP_ATTR_FK'],
                                    p_data_typ  = v_data_base_nm_dict['REP_ATTR_DATA_TYP'],
                                    p_char      = v_db_nm)
            v_collctn_nm_dict   = v_sys_data_dict['COLLECTION_NM_DICT']
            v_collection_list   = self.get_collection_list(v_db_nm)
            for v_collection_nm in v_collection_list:
                v_collctn_data_pk = self.db_api_obj.get_rep_attr_data_pk(
                                        p_master_fk = v_db_data_pk,
                                        p_attr_fk   = v_collctn_nm_dict['REP_ATTR_FK'],
                                        p_data_typ  = v_collctn_nm_dict['REP_ATTR_DATA_TYP'],
                                        p_char= v_collection_nm)
                v_doc_obj       = self.get_doc_obj_by_dt(v_db_nm,v_collection_nm,v_last_load_dt)
                self.db_api_obj.mng_load_doc(v_sys_data_dict,v_collctn_data_pk,v_doc_obj)

            self.db_api_obj.put_last_load_dt(v_db_nm,v_load_date)

        self.db_api_obj.exe_ctas_doc_init_list()

    def etl_doc_detl_nfo(self,p_batch_no):
        v_res       = self.db_api_obj.get_doc_status_init_list(p_batch_no)
        for v_row in v_res:
            v_doc_id_pk     = v_row[0]
            v_doc_id        = v_row[1]
            v_collctn_nm    = v_row[2]
            v_db_nm         = v_row[3]

            v_doc_id_dict   = self.get_doc_dict_by_id(v_db_nm,v_collctn_nm,v_doc_id)
            self.mng_doc_kvp_obj.get_dict_value(v_doc_id_pk,"**Document Dictionary**",v_doc_id_dict)
            v_rep_attr_data_pk = self.db_api_obj.get_kvp_child_pk(v_doc_id_pk,'DOCUMENT_STATUS')
            self.db_api_obj.post_rep_attr_data_value(p_rep_attr_data_pk= v_rep_attr_data_pk,p_val_char = 'COMP')

