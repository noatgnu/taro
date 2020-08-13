from tornado.httpclient import AsyncHTTPClient
from tornado import gen
from tornado.escape import url_escape
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
            header = []
            data = []
            for tr in soup.find_all('tr'):
                if not row_count:
                    for th in tr.find_all('th'):
                        header.append(th.text)
                    row_count += 1
                else:
                    r = []
                    url_parsed = urllib.parse.urlparse(tr.find_all('td')[12].a["href"])
                    print(urllib.parse.parse_qs(url_parsed.query))
                    for i, td in enumerate(tr.find_all('td')):
                        r.append(td.text)

                    data.append(r)
            df.append(pd.DataFrame(data, columns=header))

            yield gen.sleep(1)
        df = pd.concat(df, ignore_index=True)
        df.to_csv("test.txt", sep="\t", index=False)
        raise gen.Return(body)

