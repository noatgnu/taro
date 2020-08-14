from tornado.httpclient import AsyncHTTPClient
from tornado import gen
from tornado.httputil import url_concat
from tornado.escape import url_escape, json_decode
from bs4 import BeautifulSoup
import urllib.parse
import pandas as pd


class BaseParser:
    def __init__(self, url, agents):
        self.url = url
        self.agents = agents
        self.client = AsyncHTTPClient()
        self.response = None

    def set_agents(self, agents):
        self.agents = agents

    @gen.coroutine
    def get_response(self):
        self.response = yield self.client.fetch(self.url)

    @gen.coroutine
    def parse(self):
        pass


class NCEPParser(BaseParser):
    def __init__(self, url, agents):
        super().__init__(url, agents)

    @gen.coroutine
    def parse(self):
        yield self.get_response()
        body = BeautifulSoup(self.response.body, 'html.parser')

        df = []
        for row in body.find_all('td'):
            url = self.url.replace("browse.html", urllib.parse.quote(row.a["href"], safe="./:?="))
            response = yield self.client.fetch(url)
            soup = BeautifulSoup(response.body, 'html.parser')
            row_count = 0
            #header = []
            data = []
            for tr in soup.find_all('tr'):
                if not row_count:
                    #for th in tr.find_all('th'):
                        #header.append(th.text)
                    row_count += 1
                else:

                    url_parsed = urllib.parse.urlparse(urllib.parse.unquote(tr.find_all('td')[12].a["href"]))
                    r = urllib.parse.parse_qs(url_parsed.query)
                    for k in r:
                        r[k] = r[k][0]
                    #for i, td in enumerate(tr.find_all('td')):
                        #r.append(td.text)

                    data.append(r)
            df.append(pd.DataFrame(data))

            yield gen.sleep(1)
        df = pd.concat(df, ignore_index=True)
        raise gen.Return(df)


class BioMartParser(BaseParser):
    def __init__(self, url, agents):
        super().__init__(url, agents)




    @gen.coroutine
    def get_marts(self):
        response = yield self.client.fetch(self.url + "/martservice/marts.json")
        raise gen.Return(json_decode(response.body))


    @gen.coroutine
    def get_datasets(self, dataset):
        response = yield self.client.fetch(url_concat(self.url + "/martservice/datasets.json", {"config": dataset}))
        raise gen.Return(json_decode(response.body))

    @gen.coroutine
    def get_all_filters(self):
        marts = yield self.get_marts()
        result = {}
        for mart in marts:
            datasets = yield self.get_datasets(mart["name"])
            result[mart["name"]] = {}
            for dataset in datasets:
                filters = yield self.get_filters(dataset["name"], mart["name"])
                result[mart["name"]][dataset["name"]] = pd.DataFrame(filters)
        raise gen.Return(json_decode(result))

    @gen.coroutine
    def get_filters(self, dataset, config):
        filters = yield self.client.fetch(
            url_concat(self.url + "/martservice/filters.json", {"datasets": dataset, "config": config}))
        raise gen.Return(json_decode(filters.body))

    @gen.coroutine
    def get_attributes(self, dataset, config):
        attributes = yield self.client.fetch(
            url_concat(self.url + "/martservice/attributes.json", {"datasets": dataset, "config": config}))
        raise gen.Return(json_decode(attributes.body))

    @gen.coroutine
    def get_all_attributes(self):
        marts = yield self.get_marts()
        result = {}
        for mart in marts:
            datasets = yield self.get_datasets(mart["name"])
            result[mart["name"]] = {}
            for dataset in datasets:
                attributes = yield self.get_attributes(dataset["name"], mart["name"])
                result[mart["name"]][dataset["name"]] = pd.DataFrame(attributes)
        raise gen.Return(result)


