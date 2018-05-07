

from threading import Thread
import time
import json

from django.conf import settings

from crawler.celery_tasks import downloader, yelp_downloader
from crawler.models import OutLink

import requests


class CrawlerManager:
    def __init__(self, seeds=None, qs=50):
        """
        Initiate the crawler with the seeds provided. If the seeds are not provided via kwargs,
        they are taken from the DB   using the outlinks API.
        :param seeds: the seeds, if provided are used to  crawl first.
        :param qs: The queue size or the number concurrent downloaders (or celery tasks).
        """
        self.seeds = seeds
        self.workers_queue = []
        self.queue_size = qs

    def get_outlinks_from_remote(self, qs=0):
        outlinks_server_url = "http://{}/get-outlinks/?qs={}".format(settings.DATABASES.get('default').get('HOST'), qs)
        response = requests.get(outlinks_server_url)
        seeds = json.loads(response.content).get('links')
        return seeds

    def crawl(self):
        urls = self.seeds
        if not urls:
            urls = self.get_outlinks_from_remote(qs=self.queue_size)
        if urls:
            for url in urls:
                task = yelp_downloader.delay(url)
                self.workers_queue.append((task, url))
        thread = Scheduler(self.workers_queue)
        thread.start()
        while True:
            print('number of  active workers {}'.format(len(self.workers_queue)))
            free_workers_count = self.queue_size - len(self.workers_queue)
            if free_workers_count > 5:
                urls = self.get_outlinks_from_remote(qs=free_workers_count)
                if len(urls) <= 0:
                    time.sleep(5)
                    continue
                for url in urls:
                    print('adding {}'.format(url))
                    task = downloader.delay(url)
                    self.workers_queue.append((task, url))
            else:
                time.sleep(.1)


class Scheduler(Thread):

    def __init__(self, workers_queue):
        super(Scheduler, self).__init__()
        self.workers_queue = workers_queue

    def run(self):
        while True:
            for index, task in enumerate(self.workers_queue):
                if task[0].ready():
                    del self.workers_queue[index]

# celery -A crawler.celery_tasks worker --loglevel=info


class YelpCrawlerManager(CrawlerManager):

    user_agents = ["Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36",
                   "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
                   "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
                   "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
                   "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:25.0) Gecko/20100101 Firefox/25.0",
                   "Mozilla/5.0 (Windows; U; Windows NT 6.1; tr-TR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
                   "Mozilla/5.0 (Windows; U; Windows NT 6.1; ko-KR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
                   "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16",
                   "Opera/9.80 (X11; Linux i686; U; hu) Presto/2.9.168 Version/11.50",
                   "Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14"
                   "Opera/9.80 (X11; Linux x86_64; U; Ubuntu/10.10 (maverick); pl) Presto/2.7.62 Version/11.01"]

    bizs = ["restaurant",
            "club & disco",
            "pizza",
            "techno clubs",
            "Friseur",
            "Hotel",
            "Kino"
            ]

    def download_search_results(self):
        urls = self.seeds
        thread = Scheduler(self.workers_queue)
        thread.start()
        urls = self.build_search_queries()
        while True:
            print('number of  active workers {}'.format(len(self.workers_queue)))
            free_workers_count = self.queue_size - len(self.workers_queue)
            for i in range(free_workers_count):
                task = downloader.delay(urls.next())
                self.workers_queue.append((task, urls.next()))
            else:
                time.sleep(.1)

    def build_search_queries(self):
        import json
        import os
        import urllib
        from django.conf import settings
        with open(os.path.join(settings.BASE_DIR, 'german_cities.json'), 'r') as f:
            cities = json.load(f)
        for city in cities:
            for biz in self.bizs:
                path = "https://www.yelp.de/search?"
                query = urllib.urlencode({'find_desc': biz, 'find_loc': city})
                yield path + query



