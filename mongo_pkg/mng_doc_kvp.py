class MNG_Doc_KVP(object):

    def __init__(self,p_pass_phrase):
        from    util_pkg.config     import Config
        from    db_pkg.db_api       import DB_API

        self.config_obj             = Config()
        v_config_dict               = self.config_obj.get_dict()
        self.mongo_data_typ_list    = v_config_dict['mongo_data_typ']
        self.db_api_obj             = DB_API(p_pass_phrase)

    def get_value(self,p_parent_pk,p_key,p_value):
        v_date_typ = type(p_value).__name__
        if      v_date_typ == 'dict':
            self.get_dict_value(p_parent_pk,p_key,p_value)
        elif    v_date_typ == 'list':
            self.get_list_value(p_parent_pk,p_key,p_value)
        elif    p_key == '_id':
            None
        else:
            self.post_key_value(p_parent_pk,p_key,p_value)

    def get_dict_value(self,p_parent_pk,p_dict_nm,p_dict):
        if  p_dict_nm == "**Document Dictionary**":
            v_key_pk = p_parent_pk
        else:
            v_key_pk = self.post_key_value(p_parent_pk,'**Dictionary**',p_dict_nm)

        for v_key in p_dict:
            v_value= p_dict[v_key]
            self.get_value(v_key_pk,v_key,v_value)

    def get_list_value(self,p_parent_pk,p_list_nm,p_list):
        v_key_pk = self.post_key_value(p_parent_pk,'**List**',p_list_nm)

        for v_list_value in p_list:
            self.get_value(v_key_pk,p_list_nm,v_list_value)

    def post_key_value(self,p_parent_pk,p_key,p_value):

        if      p_key == '**Dictionary**':
            v_mongo_data_typ    = 'dict'
        elif    p_key == '**List**':
            v_mongo_data_typ = 'list'
        else:
            v_mongo_data_typ = type(p_value).__name__

        v_res_dict  = self.db_api_obj.get_rep_attr_nfo(p_key,v_mongo_data_typ)

        if  v_mongo_data_typ == 'bool':
            if p_value == True:
                v_value = 1
            else:
                v_value = 0
        else:
            v_value = p_value

        if      v_res_dict['REP_ATTR_DATA_TYP'] == 'CHAR':
            v_char  = v_value
            v_num   = None
            v_dt    = None
        elif    v_res_dict['REP_ATTR_DATA_TYP'] == 'NUM':
            v_char  = None
            v_num   = v_value
            v_dt    = None
        elif    v_res_dict['REP_ATTR_DATA_TYP'] == 'DATE':
            v_char  = None
            v_num   = None
            v_dt    = v_value

        v_key_pk    = self.db_api_obj.get_rep_attr_data_pk(v_res_dict['REP_ATTR_FK'],v_res_dict['REP_ATTR_DATA_TYP'],p_parent_pk,v_char,v_num,v_dt)

        return v_key_pk