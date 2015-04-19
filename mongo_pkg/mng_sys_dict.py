class MNG_Sys_Dict(object):

    def __init__(self,p_pass_phrase):
        from db_pkg.db_api      import DB_API

        self.db_api_obj = DB_API(p_pass_phrase)

    def get_dict(self):
        v_sys_data_dict         = {}
        v_db_nm_attr_dict       = self.db_api_obj.get_rep_attr_nfo('DATA_BASE_NM','SYS_CHAR')
        v_collctn_nm_attr_dict  = self.db_api_obj.get_rep_attr_nfo('COLLECTION_NM','SYS_CHAR')
        v_doc_id_attr_dict      = self.db_api_obj.get_rep_attr_nfo('DOCUMENT_ID','SYS_CHAR')
        v_doc_crt_dt_attr_dict  = self.db_api_obj.get_rep_attr_nfo('DOCUMENT_CRT_DT','SYS_DT')
        v_doc_stat_attr_dict    = self.db_api_obj.get_rep_attr_nfo('DOCUMENT_STATUS','SYS_DT')
        v_doc_sys_proc_dt_dict  = self.db_api_obj.get_rep_attr_nfo('DOCUMENT_PROC_DT','SYS_DT')

        v_sys_data_dict['DATA_BASE_NM_DICT']    = v_db_nm_attr_dict
        v_sys_data_dict['COLLECTION_NM_DICT']   = v_collctn_nm_attr_dict
        v_sys_data_dict['DOCUMENT_ID_DICT']     = v_doc_id_attr_dict
        v_sys_data_dict['DOCUMENT_CRT_DT_DICT'] = v_doc_crt_dt_attr_dict
        v_sys_data_dict['DOCUMENT_STATUS']      = v_doc_stat_attr_dict
        v_sys_data_dict['DOCUMENT_PROC_DT']     = v_doc_sys_proc_dt_dict

        return v_sys_data_dict