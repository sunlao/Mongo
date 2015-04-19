import  sys
from    db_pkg.mng_db       import DB
from    db_pkg.db_api       import DB_API

p_pass_phrase   = sys.argv[1]

db_obj          = DB(p_pass_phrase)
db_api_obj      = DB_API(p_pass_phrase)

v_sql = """select * from dual"""
v_res   = db_obj.get_sql_res(v_sql)
for v_row in v_res:
    print "***SQL dual results := %s" %(v_row)

v_func_nm       = 'TEST.GET_ZERO'
v_return_typ    =  'NUM'
v_parm_list     = ['TEST',123]
v_res           = db_obj.get_func_res(v_func_nm,v_return_typ,v_parm_list)

print "***Test Function %s v_res := %s" %(v_func_nm,int(v_res))

v_proc_nm       = 'TEST.BATCH_TEST'
v_parm_list2    = [1,3,'o_out']
v_res2          = db_obj.get_proc_res(v_proc_nm,v_parm_list2)

print "***Test procedure %s v_res := %s with input parameters := [%s,%s]" %(v_proc_nm,v_res2[2],v_res2[0],v_res2[1])

v_res   = db_api_obj.get_rep_attr_data_typ_fk('int')
print "***O_REP_ATTR_DATA_TYP_FK := '%s'" %(v_res)

v_res_dict = db_api_obj.get_rep_attr_nfo('Dict1','dict')
print "***REP_ATTR_FK := '%s' REP_ATTR_DATA_TYP := '%s'" %(v_res_dict['REP_ATTR_FK'],v_res_dict['REP_ATTR_DATA_TYP'])


db_obj.exit