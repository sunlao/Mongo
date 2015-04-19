import  cx_Oracle

class DB(object):
    def __init__(self,p_pass_phrase):
        from util_pkg.config        import Config
        from util_pkg.mng_crypto    import MNG_Crypto

        config_obj      = Config()
        decrypt_obj     = MNG_Crypto(p_pass_phrase)

        v_db_usr        = config_obj.get_value('Rep_Data_Store_USR')
        v_db_host       = config_obj.get_value('Rep_Data_Store_Host')
        v_db_port       = config_obj.get_value('Rep_Data_Store_Port')
        v_db_serv_nm    = config_obj.get_value('Rep_Data_Store_SRVR')
        v_connect_str   = "%s:%s/%s" %(v_db_host,v_db_port,v_db_serv_nm)
        v_encrypt_pwd   = config_obj.get_value('Rep_Data_Store_PWD')
        v_db_pwd        = decrypt_obj.decrypt(v_encrypt_pwd)

        self.db_obj     = cx_Oracle.connect(v_db_usr,v_db_pwd,v_connect_str)

    def exit(self):
        self.db_obj.close()

    def get_return_typ(self,p_return_typ):
        if  p_return_typ == "CHAR":
            v_out = cx_Oracle.STRING
        elif    p_return_typ == "DATE":
            v_out = cx_Oracle.DATETIME
        elif    p_return_typ == "NUM":
            v_out = cx_Oracle.NUMBER
        elif    p_return_typ == "CUR":
            v_out = cx_Oracle.CURSOR

        return v_out

    def get_sql_res(self,p_sql):
        res_obj = self.db_obj.cursor()
        v_res   = res_obj.execute(p_sql)

        return v_res

    def get_proc_res(self,p_proc_nm,p_parm_list):
        res_obj = self.db_obj.cursor()
        v_out   = res_obj.callproc(p_proc_nm,p_parm_list)

        return  v_out

    def get_func_res(self,p_func_nm,p_return_typ,p_parm_list):
        res_obj = self.db_obj.cursor()

        v_return_typ = self.get_return_typ(p_return_typ)
        v_out   = res_obj.callfunc(p_func_nm,v_return_typ,p_parm_list)

        return v_out
