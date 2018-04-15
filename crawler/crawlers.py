import thread
import time

from crawler.arrays import Queue, ThreadSafeQueue
from crawler.models import Page
from crawler.scrapper import Soup
from Queue import Queue as SysQueue


seeds = ["https://www.dastelefonbuch.de/Branchen",
         "https://www.golocal.de/deutschland/locations/",
         "https://web2.cylex.de/s?q=&c=&z=&p=1&dst=&sUrl=&cUrl=&hw=1",
         "https://www.oeffnungszeitenbuch.de/suche/filiale-Wo-50+km-x-1.html",
         "https://www.stadtbranchenbuch.com/",
         "https://www.branchenverzeichnis.org/",
         "http://branchenbuch.meinestadt.de/",
         "https://www.goyellow.de/",
         "https://www.listit.de/",
         "https://www.gelbeseiten.de/",
         "https://www.branchenverzeichnis.info/verzeichnis.html",
         "https://www.11880.com/branchen",
         "https://www.misterwhat.de/"]


class Crawler:

    def __init__(self, seed):
        self.seed = seed
        self.queue = Queue()
        self.queue.enqueue(seed)
        self.visited = set()
        self.content = []

    def crawl_bfs(self, limit=0):
        counter = 0
        start = time.time()
        while not self.queue.is_empty():
            # print 'counter ', counter
            if limit and counter > limit:
                break
            counter += 1
            url = self.queue.dequeue()
            soup = Soup(url)
            # print 'queue size: ', self.queue.size()
            self.visited.add(url)
            # print str(len(self.visited)) + 'in' + str(time.time()-start)
            # print "downloading " + str(url)
            if soup is not None:
                text = soup.get_all_p_text()
                self.content.append({'content': text, 'url': url})
                for link in soup.get_absolute_internal_links():
                    if (link not in self.visited) and (not self.is_media_file(link)):
                        self.queue.enqueue(link)
        return self.content

    @staticmethod
    def is_media_file(url):
        media_identifier_tokens = ['.jpg', '.png', 'jpeg', '.js', '.css', '.gif', '.pdf', '.doc', '.docx', '.svg']
        for token in media_identifier_tokens:
            if url.lower().endswith(token):
                return True
        return False


class CrawlerManager:

    def __init__(self, seed, workers=4, seeds=[], fetch_external=False):
        self.seed = seed
        self.visited = set()
        self.queue = Queue()
        self.outlinks_queue = SysQueue()  # to be consumed by manger and produced by spider
        self.links_queue = SysQueue()  # opposite to outlinks_queue
        self.fetch_external = fetch_external
        if seeds:
            for seed in seeds:
                self.links_queue.put(seed)
        else:
            self.queue.enqueue(seed)
        self.content = []
        self.max_workers = 10

    def crawl_bfs(self, limit=0):
        counter = 0
        for i in range(self.max_workers):
            thread.start_new_thread(spider, (i, self.links_queue, self.outlinks_queue, self.visited, self.fetch_external))
        time.sleep(5)
        # thread.start_new_thread(dumper, (self.visited,))
        # thread.start_new_thread(queue_dumper, (self.queue.queue,))
        start = time.time()
        while True:

            outlinks = self.outlinks_queue.get()
            for link in outlinks:
                if link not in self.visited:
                    self.queue.enqueue(link)
            for item in self.queue.queue:
                self.links_queue.put(item)
            # print str(len(self.visited))+ ' in '+ str(time.time()-start)+'   xxxxxxxxxxxxxxxxxxxx '
            self.outlinks_queue.task_done()

    @staticmethod
    def is_media_file(url):
        media_identifier_tokens = ['.jpg', '.png', 'jpeg', '.js', '.css', '.gif', '.pdf', '.doc', '.docx', '.svg']
        for token in media_identifier_tokens:
            if url.lower().endswith(token):
                return True
        return False


def spider(id, links_queue, outlinks_queue, visited, fetch_external):
    start = time.time()
    while True:
        url = links_queue.get()
        if url in visited:
            continue
        visited.add(url)
        soup = Soup(url)
        try:
            print "{} pages downloaded and {} is downloading {}".format(len(visited), id, url)
        except:
            pass
        # print 'queue size: ', self.queue.size()
        if soup.soup is not None:
            push_to_db(url, soup.html)
            absolute_internal_links = soup.get_absolute_internal_links()
            if fetch_external:
                external_links = soup.get_external_links()
                outlinks_queue.put(list(absolute_internal_links) + list(external_links))
            else:
                outlinks_queue.put(list(absolute_internal_links))
        links_queue.task_done()
        # print str(id) + " finished downloading " + str(url)


def push_to_db(url, html):
    try:
        page = Page(url=url, content=html)
        page.save()
    except Exception as e:
        pass
        # print url


# def dumper(visited_links):
#     import json
#     while True:
#
#         with open('visited_links.txt', 'w') as f:
#             json.dump(list(visited_links), f, indent=2)
#         time.sleep(20)
#
#
# def queue_dumper(queue):
#     import json
#
#     while True:
#         with open('queue.txt', 'w') as f:
#             json.dump(queue, f, indent=2)
#         time.sleep(30)
