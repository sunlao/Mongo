from util_pkg.config import Config

config_obj = Config()

v_key   = 'mongo_url'
v_val   = 'mongodb://localhost:27017/'

config_obj.post_config(v_key,v_val)

v_get_val = config_obj.get_value(v_key)

print "mongo url := %s" %(v_get_val)