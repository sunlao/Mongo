class MNG_Doc_Dict(object):

    def get_id(self,p_doc_dict):
        v_doc_id    = p_doc_dict['_id']

        return v_doc_id

    def get_crt_dt(self,p_doc_id):
        v_doc_crt_dt        = p_doc_id.generation_time

        return v_doc_crt_dt