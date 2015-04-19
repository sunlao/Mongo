from    util_pkg.mng_crypto     import MNG_Crypto

v_pass_phrase   = 'Test_Crypto'

crypto_obj      = MNG_Crypto(v_pass_phrase)
decrypto_obj    = MNG_Crypto(v_pass_phrase)

v_test_pwd      = "Encrypt This"
print "Test password is := %s"  %(v_test_pwd)

v_encrypt_pwd   = crypto_obj.encrypt(v_test_pwd)
print "**Encrypted password is := %s" %(v_encrypt_pwd)

v_decrypt_pwd   = decrypto_obj.decrypt(v_encrypt_pwd)
print "**Decrypted password is := %s" %(v_decrypt_pwd)