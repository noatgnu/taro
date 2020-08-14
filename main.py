from tornado.httpclient import  AsyncHTTPClient
from tornado import gen
from tornado.ioloop import IOLoop
import functools
import asyncio
from parser_handler.parser import BaseParser, NCEPParser,BioMartParser


@gen.coroutine
def get_ncEP_async():
    p = NCEPParser("http://www.jianglab.cn/ncEP/browse.html", {"email": "toan.phung@uqconnect.edu.au"})
    response = yield p.parse()
    raise gen.Return(response)

@gen.coroutine
def get_sorfs_async():
    p = BioMartParser("http://biomart.biobix.be/", {"email": "toan.phung@uqconnect.edu.au"})
    response = yield p.get_all_attributes()
    print(response)

@gen.coroutine
def send_sorfs_query():
    test_xml = """<!DOCTYPE Query><Query client="true" processor="JSON" limit="1000" header="1"><Dataset name="BioMart" config="Human"><Filter name="human__strand_104" value="1" filter_list=""/><Attribute name="human__sorf_id_104"/></Dataset></Query>"""
    client = AsyncHTTPClient()
    import urllib.parse
    response = yield client.fetch("http://biomart.biobix.be/martservice/results", method="POST", body=urllib.parse.urlencode(dict(query=test_xml)))
    print(response.body)

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    IOLoop.current().run_sync(send_sorfs_query)