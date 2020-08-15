from tornado.httpclient import  AsyncHTTPClient
from tornado import gen, web
from tornado.ioloop import IOLoop
import pandas as pd
import asyncio
from parser_handler.parser import BaseParser, NCEPParser, BioMartParser
from tornado.options import define, options

define("init", default=False, type=bool, help="Initiating the text database collection.")
define("port", default=8000, type=int, help="Server listening port")

local_database = {"ncEP": None, "smProt": None, "upep": None}

@gen.coroutine
def get_ncEP_async():
    p = NCEPParser("http://www.jianglab.cn/ncEP/browse.html", {"email": "toan.phung@uqconnect.edu.au"})
    response = yield p.parse()
    response.to_csv("ncEP.txt", sep="\t", index=False)

@gen.coroutine
def get_sorfs_async():
    p = BioMartParser("http://biomart.biobix.be/", {"email": "toan.phung@uqconnect.edu.au"})
    response = yield p.get_all_attributes()
    print(response)

@gen.coroutine
def send_sorfs_query():
    test_xml = """<!DOCTYPE Query><Query client="true" processor="JSON" limit="-1" header="1"><Dataset name="BioMart" config="Human"><Filter name="human__strand_104" value="1" filter_list=""/><Attribute name="human__sorf_id_104"/><Attribute name="human__CELL_LINE_104"/><Attribute name="human__chr_104"/><Attribute name="human__strand_104"/><Attribute name="human__sorf_begin_104"/><Attribute name="human__sorf_end_104"/><Attribute name="human__spliced_104"/><Attribute name="human__start_parts_104"/><Attribute name="human__stop_parts_104"/><Attribute name="human__sorf_length_104"/><Attribute name="human__start_codon_104"/><Attribute name="human__downstream_gene_distance_104"/><Attribute name="human__upstream_gene_distance_104"/><Attribute name="human__tr_seq_104"/><Attribute name="human__aa_seq_104"/><Attribute name="human__mass_104"/><Attribute name="human__annotation_104"/><Attribute name="human__biotype_104"/><Attribute name="human__RPKM_104"/><Attribute name="human__coverage_104"/><Attribute name="human__coverage_uniformity_104"/><Attribute name="human__exon_overlap_104"/><Attribute name="human__in_frame_coverage_104"/><Attribute name="human__in_frame_104"/><Attribute name="human__pc_exon_overlap_104"/><Attribute name="human__id_104"/><Attribute name="human__Rltm_min_Rchx_104"/><Attribute name="human__FLOSS_104"/><Attribute name="human__classification_104"/><Attribute name="human__orfscore_104"/><Attribute name="human__peak_shift_104"/><Attribute name="human__PhastCon_104"/><Attribute name="human__PhyloP_104"/><Attribute name="human__p_value_104"/></Dataset></Query>"""
    client = AsyncHTTPClient()
    import urllib.parse
    response = yield client.fetch("http://biomart.biobix.be/martservice/results", method="POST", body=urllib.parse.urlencode(dict(query=test_xml)))
    print(response.body)


class BaseHandler(web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PUT')

    def options(self):
        self.set_status(204)
        self.finish()


class DatabaseHandler(BaseHandler):
    def initialize(self, database):
        self.database = database

    def get(self):
        self.write(self.database[self.get_argument("db")].to_json(orient="records"))

@gen.coroutine
def load_local_database():
    local_database["ncEP"] = pd.read_csv("ncEP.txt", sep="\t")
    local_database["smProt"] = pd.read_csv("smProt.txt", sep="\t", encoding='unicode_escape')
    upep_db = pd.read_csv("upep.txt", sep="~", encoding='unicode_escape')
    upep_sp = pd.read_csv("upep_sp.txt", sep="~")
    upep_type = pd.read_csv("upep_type.txt", sep="\t")
    upep_db = upep_db.merge(upep_sp, left_on="species_ref_id", right_on="key")
    upep_db = upep_db.merge(upep_type[["key", "name", "description"]], left_on="spep_type_ref_id", right_on="key")
    column = [i for i in upep_db.columns if i not in ["created", "key", "key_x", "key_y", "species_ref_id", "spep_type_ref_id"]]
    local_database["upep"] = upep_db[column]


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if options.init:
        IOLoop.current().run_sync(get_ncEP_async)
    IOLoop.current().run_sync(load_local_database)
    application = web.Application([
        (r"/api/", DatabaseHandler, dict(database=local_database))
    ])
    application.listen(options.port)
    IOLoop.current().start()

