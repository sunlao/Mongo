class DB_API(object):

    def __init__(self,p_pass_phrase):
        from    db_pkg.mng_db           import DB
        from    mongo_pkg.mng_doc_dict  import MNG_Doc_Dict

        self.db_obj             = DB(p_pass_phrase)
        self.mng_doc_dict_obj   = MNG_Doc_Dict()

    def get_rep_attr_data_typ_fk(self,p_mongo_data_typ):
        v_proc_nm           = 'MNG_REP_ATTR_DATA_TYP.GET_REP_ATTR_DATA_TYP_FK'
        v_parm_list         = [p_mongo_data_typ,'o_out1']

        v_res               = self.db_obj.get_proc_res(v_proc_nm,v_parm_list)

        return v_res[1]

    def get_rep_attr_nfo(self,p_rep_attr_nm,p_mongo_data_typ):
        v_dict                          = {}
        v_proc_nm                       = 'MNG_REP_ATTR.GET_REP_ATTR_NFO'
        v_parm_list                     = [p_rep_attr_nm,p_mongo_data_typ,'o_out1','o_out2']

        v_res                           = self.db_obj.get_proc_res(v_proc_nm,v_parm_list)

        v_dict['REP_ATTR_FK']           =(v_res[2])
        v_dict['REP_ATTR_DATA_TYP']     =(v_res[3])

        return v_dict

    def get_rep_attr_data_pk(self,p_attr_fk,p_data_typ,p_master_fk=None,p_char=None,p_num=None,p_dt=None):
        v_func_nm                       = 'MNG_REP_ATTR_DATA.GET_REP_ATTR_DATA_PK'
        v_return_typ                    =  'NUM'
        v_parm_list                     = [p_master_fk,p_attr_fk,p_data_typ,p_char,p_num,p_dt]

        v_res                           = self.db_obj.get_func_res(v_func_nm,v_return_typ,v_parm_list)

        return int(v_res)

    def get_kvp_child_pk(self,p_doc_pk,p_attr_nm):
        v_func_nm                       = 'MNG_REP_ATTR_DATA.GET_KVP_CHILD_PK'
        v_return_typ                    =  'NUM'
        v_parm_list                     = [p_doc_pk,p_attr_nm]

        v_res                           = self.db_obj.get_func_res(v_func_nm,v_return_typ,v_parm_list)

        return int(v_res)

    def mng_load_doc(self,p_sys_data_dict,p_collctn_data_pk,p_doc_obj):
        for v_doc_dict in p_doc_obj:
            v_doc_id_data_dict      = self.put_doc_id(p_sys_data_dict,v_doc_dict,p_collctn_data_pk)
            self.put_doc_cr_dt(p_sys_data_dict,v_doc_id_data_dict['DOC_ID'],v_doc_id_data_dict['DOC_ID_DATA_PK'])
            self.put_doc_status_init(p_sys_data_dict,v_doc_id_data_dict['DOC_ID_DATA_PK'])
            self.put_doc_proc_dt(p_sys_data_dict,v_doc_id_data_dict['DOC_ID_DATA_PK'])

    def put_doc_id(self,p_sys_data_dict,p_doc_dict,p_collctn_data_pk):
        v_doc_id_dict       = p_sys_data_dict['DOCUMENT_ID_DICT']
        v_doc_id            = self.mng_doc_dict_obj.get_id(p_doc_dict)

        v_doc_id_data_pk    = self.get_rep_attr_data_pk(
            p_master_fk     = p_collctn_data_pk,
            p_attr_fk       = v_doc_id_dict['REP_ATTR_FK'],
            p_data_typ      = v_doc_id_dict['REP_ATTR_DATA_TYP'],
            p_char          = str(v_doc_id)
        )

        v_doc_id_data_dict                      = {}
        v_doc_id_data_dict['DOC_ID_DATA_PK']    = v_doc_id_data_pk
        v_doc_id_data_dict['DOC_ID']            = v_doc_id

        return v_doc_id_data_dict

    def put_doc_cr_dt(self,p_sys_data_dict,p_doc_id,p_doc_id_data_pk):
        v_doc_cr_dt_dict    = p_sys_data_dict['DOCUMENT_CRT_DT_DICT']
        v_doc_crt_dt        = self.mng_doc_dict_obj.get_crt_dt(p_doc_id)

        v_doc_cr_dt_data_pk = self.get_rep_attr_data_pk(
            p_master_fk     = p_doc_id_data_pk,
            p_attr_fk       = v_doc_cr_dt_dict['REP_ATTR_FK'],
            p_data_typ      = v_doc_cr_dt_dict['REP_ATTR_DATA_TYP'],
            p_dt            = v_doc_crt_dt
        )
        return v_doc_cr_dt_data_pk

    def put_doc_status_init(self,p_sys_data_dict,p_doc_id_data_pk):
        v_doc_stat_dict     = p_sys_data_dict['DOCUMENT_STATUS']
        v_doc_status_data_pk = self.get_rep_attr_data_pk(
            p_master_fk     = p_doc_id_data_pk,
            p_attr_fk       = v_doc_stat_dict['REP_ATTR_FK'],
            p_data_typ      = v_doc_stat_dict['REP_ATTR_DATA_TYP'],
            p_char          = 'INIT'
        )
        return v_doc_status_data_pk

    def post_rep_attr_data_value(self,p_rep_attr_data_pk,p_val_char = None,p_val_num = None,p_val_dt = None,):
        v_proc_nm                       = 'MNG_REP_ATTR_DATA.POST_REP_ATTR_DATA_VALUE'
        v_parm_list                     = [p_rep_attr_data_pk,p_val_char,p_val_num,p_val_dt]
        self.db_obj.get_proc_res(v_proc_nm,v_parm_list)

    def put_doc_proc_dt(self,p_sys_data_dict,p_doc_id_data_pk):
        import  datetime
        v_date = datetime.datetime.utcnow()
        v_doc_proc_dt_dict  = p_sys_data_dict['DOCUMENT_PROC_DT']
        v_doc_proc_dt_pk    = self.get_rep_attr_data_pk(
            p_master_fk     = p_doc_id_data_pk,
            p_attr_fk       = v_doc_proc_dt_dict['REP_ATTR_FK'],
            p_data_typ      = v_doc_proc_dt_dict['REP_ATTR_DATA_TYP'],
            p_dt            = v_date
        )
        return v_doc_proc_dt_pk

    def get_last_load_dt(self,p_mongo_db):
        v_func_nm       = 'MNG_MONGO_DB_LOAD_NFO.GET_LAST_LOAD_DT'
        v_return_typ    =  'DATE'
        v_parm_list     = [p_mongo_db]
        v_res           = self.db_obj.get_func_res(v_func_nm,v_return_typ,v_parm_list)

        return v_res

    def put_last_load_dt(self,p_mongo_db,p_load_dt):
        v_proc_nm                       = 'MNG_MONGO_DB_LOAD_NFO.POST_LAST_LOAD_DT'
        v_parm_list                     = [p_mongo_db,p_load_dt]
        self.db_obj.get_proc_res(v_proc_nm,v_parm_list)

    def get_doc_status_init_list(self,p_batch_no):
        v_sql = """
                    SELECT * FROM DOC_INIT_LIST WHERE BATCH_NO = %s
        """ %(p_batch_no)
        try:
            v_res = self.db_obj.get_sql_res(v_sql)
        except:
            print "No Data for batch no := %s" %(p_batch_no)

        return v_res

    def exe_ctas_doc_init_list(self):
        v_proc_nm                       = 'UTIL_CTAS.CTAS_FROM_VIEW'
        v_parm_list                     = ['DOC_INIT_LIST','MONGO_DATA','VW_DOC_INIT_LIST']
        self.db_obj.get_proc_res(v_proc_nm,v_parm_list)
