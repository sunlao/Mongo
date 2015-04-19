import  sys
from    util_pkg.mng_crypto     import MNG_Crypto
from    util_pkg.config         import Config

config_obj  = Config()

p_pass_phrase   = sys.argv[1]
p_pass_pwd      = sys.argv[2]

crypto_obj      = MNG_Crypto(p_pass_phrase)
v_encrypt_pwd   = crypto_obj.encrypt(p_pass_pwd)
v_key           = 'Rep_Data_Store_PWD'

config_obj.post_config(v_key,v_encrypt_pwd)
