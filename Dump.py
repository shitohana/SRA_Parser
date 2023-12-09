import asyncio
import itertools
import time
from xml.etree import ElementTree as ET
import pandas as pd
import aiohttp
from collections import namedtuple

import xmltodict

API_LIMIT = 7
NO_API_LIMIT = 2

RequestItem = namedtuple("RequestItem", {"url", "params"}, defaults=(None,) * 2)


class Dump:
    def __init__(self, api_key: str = None):
        self.__session = aiohttp.ClientSession()
        self.__semaphore = asyncio.BoundedSemaphore(API_LIMIT if api_key is not None else NO_API_LIMIT)
        self.__api_key = api_key

    async def __get(self, item: RequestItem, limit=0.0):
        async with self.__semaphore:
            start = time.time()
            response = await self.__session.get(item.url, params=item.params)
            end = time.time()

            if end - start < limit:
                await asyncio.sleep(end - start)
            return response

    async def __run(self, items: list[RequestItem]):
        test = await self.__get(items[0])

        tasks = await self.__retry_till_success(self.__get, [(item, 1.0) for item in items])

        return tasks

    def __search_wrapper(self, db, term):
        item = RequestItem(
            url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params=self.__add_api_key({"db": db, "term": term})
        )
        return item

    def __fetch_wrapper(self, db, id, rettype="xml"):
        item = RequestItem(
            url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
            params=self.__add_api_key({"db": db, "id": id, "rettype": rettype})
        )
        return item

    def __add_api_key(self, params: dict):
        return params | {"api_key": self.__api_key}

    @staticmethod
    def __batch_ids(ids: list, batch_size: int, sep=","):
        return [sep.join(map(str, ids[i:i+batch_size])) for i in range(0, len(ids), batch_size)]

    @staticmethod
    async def __retry_till_success(coro, args_list):
        tasks = {
            asyncio.ensure_future(coro(*args)): {"coro": coro, "args": args} for args in args_list
        }
        pending = tasks
        result = []

        while pending:
            finished, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_EXCEPTION)
            for task in finished:
                if task.exception():
                    print(f"{task} got {task.exception()}, retrying")
                    coro, args = tasks[task].values()
                    new_task = asyncio.ensure_future(coro(*args))
                    tasks[new_task] = {"coro": coro, "args": args}
                    pending.add(new_task)
                else:
                    result.append(task.result())

        return result

    async def search_query(self, db, term: str | list, batch_size=1, sep = "+"):
        queries = self.__batch_ids(
            [term] if type(term) is not list else term,
            batch_size, sep
        )
        items = [self.__search_wrapper(db, query) for query in queries]

        return await self.__run(items)

    async def fetch_query(self, db, id, rettype="xml", batch_size=10):
        queries = self.__batch_ids(
            [id] if type(id) is not list else id,
            batch_size, ","
        )
        items = [self.__fetch_wrapper(db, query, rettype) for query in queries]

        return await self.__run(items)

    async def close(self):
        await self.__session.close()


def parse_sra_ids(text: str):
    xml = ET.fromstring(text)
    return [element.text for element in xml.findall(".//IdList/Id")]


def parse_fetch_result(text: str):
    converted = xmltodict.parse(text)
    experiments = converted["EXPERIMENT_PACKAGE_SET"]["EXPERIMENT_PACKAGE"]
    if experiments is not None:
        return [pd.json_normalize(experiment) for experiment in experiments]
    else:
        return None



########################################################################################################################


def parse_attributes(df: pd.DataFrame):
    tag_col = 'SAMPLE.SAMPLE_ATTRIBUTES.SAMPLE_ATTRIBUTE'
    # separate list into rows
    exploded = df.explode(tag_col)
    # separate dict values to columns
    exploded = pd.concat([exploded, exploded[tag_col].apply(pd.Series)])
    # delete unparsed tag column
    return exploded.drop(columns=tag_col)


async def dump_sra(filepath: str):
    with open(filepath, "r") as file:
        srp_list = file.read().split("\n")
    api = Dump()  # you can specify api_key here

    search_results = await api.search_query("sra", term=srp_list, batch_size=10, sep=" OR ")

    ncbi_ids = list(itertools.chain(*[parse_sra_ids(await resp.text()) for resp in search_results]))

    fetch_results = await api.fetch_query("sra", id=ncbi_ids, batch_size=50)

    experiments = list(itertools.chain(*[parse_fetch_result(await resp.text()) for resp in fetch_results]))
    df = pd.concat(experiments)

    await api.close()
    return df


if __name__ == '__main__':
    df = asyncio.run(dump_sra("TE_SRP_list.txt"))

    df_expanded = parse_attributes(df)

    pass