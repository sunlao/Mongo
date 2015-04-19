import  base64
from    Crypto.Cipher   import AES
from    Crypto.Util     import Counter

class MNG_Crypto(object):

    def __init__ (self,p_pass_phrase):
        self.v_key = p_pass_phrase.ljust(32,'X')

    def encrypt(self,p_plain_txt):
        if len(p_plain_txt) % 16 != 0:
            p_plain_txt += ' ' * (16 - len(p_plain_txt) % 16)
        v_ctr           = Counter.new(128)
        v_cipher    = AES.new(self.v_key, AES.MODE_CTR, counter=v_ctr)
        v_cipher_txt    = base64.b64encode(v_cipher.encrypt(p_plain_txt))

        return v_cipher_txt

    def decrypt(self,p_cipher_txt):
        v_ctr           = Counter.new(128)
        v_cipher        = AES.new(self.v_key, AES.MODE_CTR, counter=v_ctr)
        v_plain_txt = v_cipher.decrypt(base64.b64decode(p_cipher_txt))
        v_plain_txt = v_plain_txt.strip()

        return v_plain_txt