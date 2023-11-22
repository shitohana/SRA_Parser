import asyncio
import aiohttp
from xml.etree import ElementTree as ET
from itertools import chain
import xmltodict
import pandas as pd


class Dump:
    def __init__(self, api_key: str = None):
        """
        :param api_key: entrez api key -> allows maximum of 10 simultaneous requests per second
        """
        self.api_key = api_key
        self.session = aiohttp.ClientSession()

        self.__rate_limit = 3 if self.api_key is None else 10
        self.__sem = asyncio.Semaphore(self.__rate_limit)

    async def search_sra(self, term):
        """
        Search for study ids
        :param term: search string in entrez format: e.g. "SRR123+SRR128"
        :return: list of ids
        """
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        params = {
            "db": "sra",
            "term": term,
        }
        if self.api_key is not None:
            params["api_key"] = self.api_key

        async with self.__sem:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    print("Search  ", term)
                    xml = ET.fromstring(await resp.text())  # get xml response
                    ids = [int(element.text) for element in xml.findall(".//IdList/Id")]  # get ids from xml
                    if ids:
                        await asyncio.sleep(1)  # for rate limit per second
                        return ids
                    else:
                        raise Exception("Empty response")
                raise Exception(f"Status is {resp.status}")

    async def fetch_sra(self, term: str):
        """
        Search for study ids
        :param term: comma separated ids
        :return: list of dict
        """
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "sra",
            "id": term
        }
        if self.api_key is not None:
            params["api_key"] = self.api_key

        async with self.__sem:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    print("Fetching ", term)
                    data = xmltodict.parse(await resp.text())  # parse xml, turn to dict
                    try:
                        await asyncio.sleep(1)  # for rate limit per second
                        return data["EXPERIMENT_PACKAGE_SET"]["EXPERIMENT_PACKAGE"]
                    except KeyError:
                        raise Exception(f"Bad response for {resp.raw_url}")
                raise Exception(f"Status is {resp.status}")

    async def fetch_list(self, ids, batch_size=10):
        """
        :param ids: entrez ids
        :param batch_size: ids in one request (200 is max)
        :return DataFrame with merged responses
        """
        # split ids to batches for less requests
        batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
        # prepare search strings from batches
        terms = [",".join([str(value) for value in batch]) for batch in batches]
        # try untill all success
        result = await self.__retry_till_success(self.fetch_sra, [[term] for term in terms])
        # normalize and concat into df
        return pd.concat([pd.json_normalize(res) for res in list(chain(*result))])

    async def search_list(self, ids, batch_size=10):
        """
        :param ids: study ids e.g. SRA123456
        :param batch_size: ids in one request limit
        :return list with ids
        """
        # filter empty string
        ids = list(filter(lambda value: value, ids))
        # split ids to batches for fewer requests
        batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
        # prepare search strings from batches
        terms = [" OR ".join([str(value) for value in batch]) for batch in batches]
        # try until all success
        result = await self.__retry_till_success(self.search_sra, [[term] for term in terms])
        return list(chain(*result))

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


def parse_attributes(df: pd.DataFrame):
    """
    unnest tag column
    """
    tag_col = 'SAMPLE.SAMPLE_ATTRIBUTES.SAMPLE_ATTRIBUTE'
    # tag columns is a list of dictionaries, so we firstly
    # separate list into rows
    exploded = df.explode(tag_col)
    # separate dict values to columns
    exploded = pd.concat([exploded, exploded[tag_col].apply(pd.Series)])
    # delete unparsed tag column
    return exploded.drop(columns = tag_col)


# USAGE
if __name__ == '__main__':
    async def main():
        # create class
        parser = Dump()
        with open("TE_SRP_list.txt", "r") as file:
            srp_list = file.read().split("\n")
        # get ids of experiments from selected studies
        ids = await parser.search_list(srp_list)
        # gather metadata of experiments into dataframe
        experiments = await parser.fetch_list(ids)
        # parse tag column into separate columns (if needed)
        parsed = parse_attributes(experiments)
        # close aiohttp session
        await parser.session.close()


    asyncio.run(main())
