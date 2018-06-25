from threading import Thread
import random
from urlparse import urlparse

from django.utils import timezone
from django.conf import settings

from scrapper import Soup
from crawler.models import OutLink, Page, Domain, get_url_hash
from user_agents import user_agents

import requests


class Downloader(Thread):

    def __init__(self, url, using_tor=True):
        super(Downloader, self).__init__()
        self.url = url
        self.using_tor = using_tor

    def run(self):
        urls_bulk = []
        outlinks_bulk = []
        print 'downloading {}'.format(self.url)
        url = self.url
        outlink_obj, created = OutLink.objects.get_or_create(url=url)
        if url.startswith("mailto"):
            return
        # ################### CHECK DOMAIN TIMEOUT  ########################
        url_parse_object = urlparse(url)
        domain_string = url_parse_object.netloc
        try:
            domain, created = Domain.objects.get_or_create(domain=domain_string)
            if not created and domain.timeout and domain.last_attemp:
                if domain.last_attempt + timezone.timedelta(seconds=settings.REQUEST_DOMAIN_FALLBACK_TIME) < timezone.now():
                    print('still in timeout')
                    return
        except Exception as e:
            print "something went wrong at while checking if domain in timeout: {}".format(url)
            return
        if outlink_obj.download_status != OutLink.DownloadStatus.Completed:
            try:
                Page.objects.using('pages').get(url_hash=get_url_hash(url))
                outlink_obj.download_status = True
                outlink_obj.save()
                print 'outlink download status changed'
                return
            except Page.DoesNotExist:


                session = requests.session()
                session.headers = {'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
                if self.using_tor:
                    session.proxies = {
                        'http': 'socks5h://localhost:9050',
                        'https': 'socks5h://localhost:9050'
                    }
                    response = session.get(url)
                else:
                    response = session.get(url)
                if response.status_code == 200:
                    soup = Soup(url, response)
                    if soup.soup:
                        outlink_obj.download_status = OutLink.DownloadStatus.Completed
                        outlink_obj.save()
                        page, created = Page.objects.using('pages').get_or_create(url=url, content=soup.html)
                        # print 'Page object created {}'.format(url)
                        if not created:
                            return
                        external_urls = soup.get_absolute_internal_links()
                        for url in external_urls:
                            if url.startswith("mailto") or url.startswith("tel:") or url.startswith("javascript:"):
                                continue
                            elif '#' in url:
                                if urlparse(url).path == url_parse_object.path:
                                    continue
                                url = url.split('#')[0]
                            url_hash = get_url_hash(url)

                            # ### for performance boosting keep some local info about the outlinks.
                            # If the outlink exists locally then do not write at the remote DB. This will make the
                            # remote less busy.
                            outlink, created = OutLink.objects.using('pages').get_or_create(url_hash=url_hash, url=url)
                            if created:
                                urls_bulk.append(url)

                        for url in urls_bulk:
                                outlinks_bulk.append(
                                    OutLink(
                                        url=url,
                                        url_hash=get_url_hash(url)
                                    )
                                )
                        try:
                            OutLink.objects.bulk_create(outlinks_bulk)
                        except:
                            try:
                                for url in urls_bulk:
                                    OutLink.objects.create(url=url)
                            except Exception as e:
                                print(e)
                        print '{} outlinks created '.format(len(urls_bulk))
                elif 300 <= response.status_code < 400:
                    outlink_obj.download_status = True
                    outlink_obj.is_300 = True

                elif 400 <= response.status_code < 500:
                    outlink_obj.download_status = True
                    outlink_obj.is_404 = True

                elif 500 <= response.status_code < 600:
                    outlink_obj.download_status = True
                    outlink_obj.is_500 = True
                else:
                    outlink_obj.download_status = True
                outlink_obj.save()
