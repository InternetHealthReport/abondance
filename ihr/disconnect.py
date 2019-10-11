import os
import arrow
import json
import logging
import pickle
import math
from collections import defaultdict
from requests_futures.sessions import FuturesSession


def worker_task(resp, *args, **kwargs):
    """Process json in background"""
    try:
        resp.data = resp.json()
    except json.decoder.JSONDecodeError:
        logging.error("Error while reading Atlas json data.\n")
        resp.data = {}


class Disconnect():
    def __init__(self, start=None, end=None, streamnames=None, af=4, session=None,
                 cache=True, cache_dir="cache/",
                 url='https://ihr.iijlab.net/ihr/api/disco_events/',
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


        if isinstance(streamnames, int) or isinstance(streamnames, str):
            streamnames = [streamnames]
        elif streamnames is None:
            streamnames = [None]

        self.streamnames = set(streamnames)
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

    def query_api(self, streamname, page):
        """Single API query. Don't call this method, use get_results instead."""

        params = dict(
            starttime__gte=arrow.get(self.start),
            endtime__lte=arrow.get(self.end),
            af=self.af,
            page=page,
            format="json"
        )

        if streamname is not None:
            params["streamname"] = streamname

        logging.info("query results for {}, page={}".format(streamname, page))
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
        for streamname in self.streamnames:
            # Skip the query if we have the corresponding cache
            cache_fname = "{}/Disconnect_start{}_end{}_streamname{}_af{}.pickle".format(
                self.cache_dir, self.start, self.end, streamname, self.af)
            if self.cache and os.path.exists(cache_fname):
                continue
            queries[streamname] = self.query_api(streamname, 1)

        # Fetch the results

        for streamname in self.streamnames:
            cache_fname = "{}/Disconnect_start{}_end{}_streamname{}_af{}.pickle".format(
                self.cache_dir, self.start, self.end, streamname, self.af)

            if self.cache and os.path.exists(cache_fname):
                #  get results from cache
                logging.info("Get results from cache")
                for res in pickle.load(open(cache_fname, "rb")):
                    yield res

            else:
                # fetch results
                all_results = []
                resp = queries[streamname].result()
                logging.info("got results for {}".format(streamname))
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
                        pages_queries.append(self.query_api(streamname, p))

                    for i, page_resp in enumerate(pages_queries):
                        resp = page_resp.result()
                        if resp.ok and "results" in resp.data and len(resp.data["results"]) > 0:
                            yield resp.data["results"]
                            all_results.append(resp.data["results"])
                        else:
                            logging.warning("No hegemony results for {}, page={}".format(self.params, i + 2))

                if self.cache and len(all_results) > 0 and len(all_results[0]):
                    logging.info("caching results to disk")
                    pickle.dump(all_results, open(cache_fname, "wb"))


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename="Disco_event.log", level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    res = Disconnect(streamnames='MX', start="2017-03-02T14:28:07", end="2017-03-03T14:28:07").get_results()

    for r in res:
        print(r)
