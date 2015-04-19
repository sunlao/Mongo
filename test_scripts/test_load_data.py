import  sys
import  datetime
from    mongo_pkg.mng_db      import MNG_DB
from    mongo_pkg.mng_doc_kvp       import MNG_Doc_KVP
from    util_pkg.config             import Config

p_pass_phrase   = sys.argv[1]

mongo_mgt_obj   = MNG_DB(p_pass_phrase)
mng_doc_kvp_obj = MNG_Doc_KVP(p_pass_phrase)
config_obj      = Config()

v_db    = 'TestDB'
v_col   = 'TestCollection'

v_date = datetime.datetime.utcnow()

v_new_doc = {
"Test Text":"This is text",
"This is number1":1234,
"This is number2":1234.098098,
"This is number3":123498769876098098,
"This is booelan T":True,
"This is booelan F":False,
"this is data":v_date,
"this is a List":['LIST1','LIST2','LIST3'],
"this is Dict":{'key1':'VAL1','key2':'val2','key3':'key3'},
"this is Dict2":{'key1':'VAL1','dict list':['list val 1',6,False,12.4],'key3':'val3'},
"Dict2":{"Dict3":"Abc"}
}

# v_mongo_data_typ_list = config_obj.get_value('mongo_data_typ')
#
# if v_mongo_data_typ_list == []:
#     print "Empty"
# else:
#     for v_mongo_data_typ in v_mongo_data_typ_list:
#         print "Data Type := %s" %(v_mongo_data_typ)

# v_res_id  = mongo_mgt_obj.put_doc('TestDB','TestCollection',v_new_doc)

v_res_id = '552851900f680d540804c1a2'
print v_res_id

v_mongo_dict = mongo_mgt_obj.get_doc_dict_by_id('TestDB','TestCollection',v_res_id)

# mng_doc_kvp_obj.get_dict_value('root','Dict1',v_mongo_dict)

# v_mongo_data_typ_list = config_obj.get_value('mongo_data_typ')
# for v_mongo_data_typ in v_mongo_data_typ_list:
#     print "Data Type := %s" %(v_mongo_data_typ)