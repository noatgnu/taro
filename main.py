from tornado.httpclient import  AsyncHTTPClient
from tornado import gen
from tornado.ioloop import IOLoop
import functools
import asyncio
from parser_handler.parser import BaseParser, NCEPParser


@gen.coroutine
def get_ncEP_async():
    p = NCEPParser("http://www.jianglab.cn/ncEP/browse.html", {"email": "toan.phung@uqconnect.edu.au"})
    response = yield p.parse()
    raise gen.Return(response)

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    IOLoop.current().run_sync(get_ncEP_async)