from threading import Thread
import time
import json

from django.conf import settings

from crawler.celery_tasks import downloader, yelp_downloader
from crawler.models import OutLink

import requests


class CrawlerManager:
    def __init__(self, seeds=None, qs=50):

        if not seeds:
            try:
                seeds = self.get_outlinks_from_remote()
            except Exception as e:
                print e

        self.seeds = seeds
        self.workers_queue = []
        self.queue_size = qs
        for seed in seeds:
            task = downloader.delay(seed)
            self.workers_queue.append((task, seed))

    def get_outlinks_from_remote(self, qs=0):
        outlinks_server_url = "http://{}/get-outlinks/?len={}".format(settings.DATABASES.get('default').get('HOST'), qs)
        response = requests.get(outlinks_server_url)
        seeds = json.loads(response.content).get('links')
        return seeds

    def crawl(self):
        if not self.seeds:
            print 'Sorry. There were no seeds. Crawler is exiting....'
            return
        thread = Scheduler(self.workers_queue)
        thread.start()
        while True:
            print 'len of queue ', len(self.workers_queue)
            if len(self.workers_queue) < self.queue_size:
                free_workers_count = self.queue_size - len(self.workers_queue)
                if free_workers_count > 5:
                    outlinks = self.get_outlinks_from_remote(qs=free_workers_count)
                    if len(outlinks) <= 0:
                        time.sleep(2)
                        continue
                    for outlink in outlinks:
                        print 'adding ', outlink
                        task = downloader.delay(outlink)
                        self.workers_queue.append((task, outlink))


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


class YelpCrawlerManager:
    bizs = ["restaurant",
            "club & disco",
            "pizza",
            "techno clubs",
            "Friseur",
            "Hotel",
            "Kino"
            ]

    def __init__(self, seeds=None, qs=100):

        if not seeds:
            try:
                seeds = list(OutLink.objects.filter(download_status=False).order_by('created_at').values_list('url', flat=True)[0:100])
            except:
                try:
                    seeds = list(OutLink.objects.filter(download_status=False).order_by('created_at').values_list('url', flat=True)[0:10])
                except:
                    return
        self.seeds = seeds
        self.workers_queue = []
        self.queue_size = qs
        for seed in seeds:
            task = yelp_downloader.delay(seed)
            self.workers_queue.append((task, seed))

    def crawl(self):
        if not self.seeds:
            print 'Sorry. There were no seeds.'
            return
        thread = Scheduler(self.workers_queue)
        thread.start()
        while True:
            print 'len of queue ', len(self.workers_queue)
            if len(self.workers_queue) < self.queue_size:
                outlinks = OutLink.objects.filter(download_status=False)
                print 'outl-inks found ', outlinks.count()
                if outlinks.count() == 0:
                    print 'no out-links, i am sleeping'
                    time.sleep(1)
                    continue
                else:
                    free_workers_count = min(outlinks.count(), self.queue_size - len(self.workers_queue))
                    print 'min value was ', free_workers_count
                    if free_workers_count < 0:
                        time.sleep(2)
                    else:
                        for outlink in outlinks[0:free_workers_count]:
                            print 'adding ', outlink.url
                            task = yelp_downloader.delay(outlink.url)
                            self.workers_queue.append((task, outlink.url))
            else:
                time.sleep(1)
                continue

    @classmethod
    def build_queries(self):
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
                yield path+query


