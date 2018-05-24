import json

from django.conf import settings

from tasks import Downloader

import requests


class TCrawlerManager:
    def __init__(self, seeds=None, qs=10, using_tor=True):
        self.seeds = seeds
        self.workers_queue = []
        self.queue_size = qs
        self.using_tor = using_tor

    def get_outlinks_from_remote(self, qs=0):
        outlinks_server_url = "http://{}/get-outlinks/?qs={}".format(settings.DATABASES.get('default').get('HOST'), qs)
        response = requests.get(outlinks_server_url)
        seeds = json.loads(response.content).get('links')
        return seeds

    def crawl(self):
        thread_pool = []
        urls = self.seeds
        if not urls:
            urls = self.get_outlinks_from_remote(qs=self.queue_size)
        if urls:
            for url in urls:
                d = Downloader(url, using_tor=self.using_tor)
                thread_pool.append(d)
                d.start()
        while True:
            for index, worker in enumerate(thread_pool):
                if not worker.is_alive():
                    del thread_pool[index]
            free_workers_count = self.queue_size - len(thread_pool)
            if free_workers_count >= 3:
                urls = self.get_outlinks_from_remote(qs=free_workers_count)
                for url in urls:
                    d = Downloader(url)
                    d.start()
                    thread_pool.append(d)

