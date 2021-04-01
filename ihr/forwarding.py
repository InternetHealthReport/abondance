import os
import arrow
from json.decoder import JSONDecodeError
import logging
import ujson as json
import math
from collections import defaultdict
from requests_futures.sessions import FuturesSession


def worker_task(resp, *args, **kwargs):
    """Process json in background"""
    try:
        resp.data = resp.json()
    except JSONDecodeError:
        logging.error("Error while reading Atlas json data.\n")
        resp.data = {}


class Forwarding():
    def __init__(self, start, end, asns=None, af=4, session=None,
                 cache=True, cache_dir="cache/",
                 url='https://ihr.iijlab.net/ihr/api/forwarding/',
                 nb_threads=2):
        """
        :originasn: Origin ASN of interest. It can be a list of ASNs or a single
        int value. Set to 0 for global hegemony.
        :start: Start date/time.
        :end: End date/time.
        :asn: Return dependency only to the given ASNs. By default return all
        dependencies.
        :af: Adress family, default is IPv4
        :session: Requests session to use
        :page: Page number for paginated results.
        :cache: Set to False to ignore cache

        :cache_dir: Directory used for cached results.
        :url: API root url
        :nb_threads: Maximum number of parallel downloads

        Notes: By default results are cached on disk.
        """


        if isinstance(asns, int):
            asns = [asns]
        elif asns is None:
            asns = [None]

        self.asns = set(asns)
        self.start = start
        self.end = end
        self.af = af
        self.session = session
        self.cache = cache
        if session is None:
            self.session = FuturesSession(max_workers=nb_threads)
        else:
            self.session = session

        self.url = url
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        self.params = {}
        self.queries = defaultdict(list)

    def query_api(self, asn, page):
        """Single API query. Don't call this method, use get_results instead."""

        params = dict(
            timebin__gte=arrow.get(self.start),
            timebin__lte=arrow.get(self.end),
            af=self.af,
            page=page,
            format="json"
        )

        if asn is not None:
            params["asn"] = asn
        else:
            logging.error("You should give an ASN.")
            return None

        logging.info("query results for {}, page={}".format(asn, page))
        self.params = params
        return self.session.get(
            url=self.url, params=params,
            hooks={'response': worker_task, }
        )

    def get_results(self):
        """Fetch AS dependencies (aka AS hegemony) results.

        Return AS dependencies for the given origin AS between the start and
        end dates.

        :returns: Dictionary of AS dependencies.

        """

        # Main loop
        queries = {}

        # Query the API
        for asn in self.asns:
            # Skip the query if we have the corresponding cache
            cache_fname = "{}/FA_start{}_end{}_asn{}_af{}.json".format(
                self.cache_dir, self.start, self.end, asn, self.af)
            if self.cache and os.path.exists(cache_fname):
                continue
            queries[asn] = self.query_api(asn, 1)

        # Fetch the results

        for asn in self.asns:
            cache_fname = "{}/FA_start{}_end{}_asn{}_af{}.json".format(
                self.cache_dir, self.start, self.end, asn, self.af)

            if self.cache and os.path.exists(cache_fname):
                #  get results from cache
                logging.info("Get results from cache")
                for res in json.load(open(cache_fname, "r")):
                    yield res

            else:
                # fetch results
                all_results = []
                resp = queries[asn].result()
                logging.info("got results for {}".format(asn))
                if resp.ok and "results" in resp.data and len(resp.data["results"]) > 0:
                    yield resp.data["results"]
                    all_results.append(resp.data["results"])
                else:
                    logging.warning("No Delay results for  {}".format(self.params))

                # if results are incomplete get the other pages
                if resp.data.get("next"):
                    nb_pages = math.ceil(resp.data["count"] / len(resp.data["results"]))
                    pages_queries = []
                    logging.info("{} more pages to query".format(nb_pages))
                    for p in range(2, int(nb_pages + 1)):
                        pages_queries.append(self.query_api(asn, p))

                    for i, page_resp in enumerate(pages_queries):
                        resp = page_resp.result()
                        if resp.ok and "results" in resp.data and len(resp.data["results"]) > 0:
                            yield resp.data["results"]
                            all_results.append(resp.data["results"])
                        else:
                            logging.warning("No hegemony results for {}, page={}".format(self.params, i + 2))

                if self.cache and len(all_results) > 0 and len(all_results[0]):
                    logging.info("caching results to disk")
                    json.dump(all_results, open(cache_fname, "w"))


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename="Forwarding_Alarms.log", level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    res = Forwarding(
        asns=[2907, 7922], start="2018-09-15", end="2018-10-16"
    ).get_results()

    for r in res:
        print(r[0])
