import os
import logging
import ujson as json
import math
from simplejson.errors import JSONDecodeError
import arrow
from requests_futures.sessions import FuturesSession


def worker_task(resp, *args, **kwargs):
    """Process json in background"""
    try:
        resp.data = resp.json()
    except JSONDecodeError:
        logging.error("Error while reading Atlas json data.\n")
        resp.data = {}


class Hegemony:

    def __init__(self, start, end, originasns=None, asns=None, af=4, session=None,
                cache=True, cache_dir="cache/",
                url='https://ihr.iijlab.net/ihr/api/hegemony/',
                nb_threads=2):
        """
        Initialize Hegemony object with parameters.
        """
        if isinstance(asns, int):
            asns = [asns]
        elif asns is None:
            asns = []

        if isinstance(originasns, int):
            originasns = [originasns]
        elif originasns is None:
            originasns = []


        self.originasns = set(originasns)
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

    
    def query_api(self, originasns, asns, page):
        """Single API query. Don't call this method, use get_results instead."""

        params = dict(
            timebin__gte=arrow.get(self.start),
            timebin__lte=arrow.get(self.end),
            af=self.af,
            page=page,
            format="json"
        )
        
        # add asn and originasn parameters to the query parameters if asns and originasns are provided respectively.

        if asns:
            params["asn"] = ",".join(map(str, asns))

        if originasns:
            params["originasn"] = ",".join(map(str, originasns))
            

        if originasns is None and asns is None:
            logging.error("You should give at least a origin ASN or an ASN.")
            return None
        self.params = params
        logging.info("query results for {}, page={}".format((originasns,asns), page))
        
        '''This sends the API request asynchronously using FuturesSession.get method and specifies
        worker_task as a hook to process the response in the background.'''
        
        return self.session.get(
                url=self.url, params=params,
                hooks={'response': worker_task, }
                )
        
        
    def get_results(self):
        """Fetch AS dependencies results."""
        # Skip the query if we have the corresponding cache
        cache_fname = "{}/hege_originasns{}_start{}_end{}_asns{}_af{}.json".format(
            self.cache_dir, "_".join(map(str, self.originasns)), self.start, self.end,
            "_".join(map(str, self.asns)), self.af
        )
        #constructs the cache file name based on provided parameters.
        if self.cache and os.path.exists(cache_fname):
            #  get results from cache
            logging.info("Get results from cache")
            with open(cache_fname, "r") as cache_file:
                for res in json.load(cache_file):
                    yield res
        else:
            all_results=[]
            resp = self.query_api(self.originasns,self.asns, 1).result()
            if resp.ok and "results" in resp.data and len(resp.data["results"])>0:
                yield resp.data["results"]
                all_results.append(resp.data["results"])
            else:
                logging.warning("No hegemony results for  origin AS={}, AS={}".format(self.originasns, self.asns))

            # if results are incomplete get the other pages
            if resp.data.get("next") :
                nb_pages = math.ceil(resp.data["count"]/len(resp.data["results"]))
                pages_queries = []
                logging.info("{} more pages to query".format(nb_pages))
                for p in range(2,int(nb_pages)+1):
                    pages_queries.append(self.query_api(self.originasns, self.asns, p))

                for i, page_resp in enumerate(pages_queries):
                    resp= page_resp.result()
                    if resp.ok and "results" in resp.data and len(resp.data["results"])>0:
                        yield resp.data["results"]
                        all_results.append(resp.data["results"])
                    else:
                        logging.warning("No hegemony results for origin AS={}, AS={}, page={}".format(self.originasns, self.asns, i+2))
                        
            '''
            If caching is disabled or the cache file doesn't exist, it queries the API for results.
            If successful, it yields the results and appends them to all_results, otherwise,
            it logs a warning message.
            '''
            if self.cache and len(all_results)>0 and len(all_results[0]) :
                logging.info("caching results to disk")
                json.dump(all_results, open(cache_fname, "w"),indent=4) #added indentation


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename="hegemony.log", level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S')
    res = Hegemony(
            originasns=[2907, 7922], start="2019-09-15", end="2019-09-16"
            ).get_results()
    json_data=" "
    for r in res:
        print(json.dumps(r,indent=4)) #added indentation
        
    

