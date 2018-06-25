from threading import Thread
import time
import json

from django.conf import settings

from crawler.celery_tasks import downloader
from utils import Downloader

import requests


class CrawlerManager:
    def __init__(self, seeds=None, qs=10):
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
                task = downloader.delay(url)
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
                    try:
                        print('adding {}'.format(url))
                    except:
                        pass
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





