from threading import Thread
import time
import json
from crawler.celery_tasks import worker
from crawler.models import OutLink


class CrawlerManager:
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
            task = worker.delay(seed)
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
                outlinks = OutLink.objects.filter(download_status=False).order_by('created_at')
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
                            task = worker.delay(outlink.url)
                            self.workers_queue.append((task, outlink.url))
            else:
                time.sleep(1)
                continue


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
