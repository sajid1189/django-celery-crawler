import hashlib
import os
import random
from urlparse import urlparse
from celery import Celery

from django.utils import timezone
from django.conf import settings

import requests
import validators
from scrapper import Soup

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_crawler.settings')

app = Celery('crawler', broker="amqp://localhost", backend='rpc://localhost')

#
# @app.task
# def demo(x):
#     l = [7, 4, 1]
#     os.chdir('/Users/sajidur/Desktop')
#     time.sleep(l[x])
#     print x, ' finished'

from user_agents import user_agents


@app.task
def downloader(url, tor=True):
    from crawler.models import OutLink, Page, Domain, get_url_hash
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

            try:
                session = requests.session()
                session.headers = {'User-Agent': user_agents[random.randint(0, len(user_agents))]}
                if tor:
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
                        try:
                            Page.objects.using('pages').create(url=url, content=soup.html)
                            print 'Page object created {}'.format(url)
                        except Exception as e:
                            print "page object could not be created: {}".format(url)

                        outlinks = soup.get_absolute_internal_links()
                        for outlink in outlinks:
                            if outlink.startswith("mailto") or outlink.startswith("tel:") or outlink.startswith("javascript:"):
                                continue
                            elif '#' in outlink:
                                if urlparse(outlink).path == url_parse_object.path:
                                    continue
                                outlink = outlink.split('#')[0]
                            try:
                                url_hash = get_url_hash(outlink)
                                OutLink.objects.get(url_hash=url_hash)

                            except OutLink.DoesNotExist:
                                outlink = OutLink.objects.create(url=outlink)
                                # print 'outlink created {}'.format(outlink)

                            except Exception as e:
                                print e
                                print 'outlink was not created'

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

            except Exception as e:
                print unicode(e)


@app.task
def flat_downloader(url, tor=True):
    # TODO check if the url is available for download.
    if not validators.url(url):
        return
    response = None

    if tor:
        session = requests.session()
        session.proxies = {}
        session.proxies['http'] = "socks5h://localhost:9050"
        session.proxies['https'] = "'socks5h://localhost:9050'"
        try:
            response = session.get(url)
        except:
            print("error while requesting page with tor.")
    else:
        try:
            response = requests.get(url)
        except:
            print("error while requesting without tor")

    if response is None:
        return

    # TODO write the page object in the local database.
    return


@app.task
def yelp_downloader(url, tor=True):

    if not validators.url(url):
        return

    # TODO check if the url is available for download.

    parsed_url = urlparse(url)
    if parsed_url.netloc != "www.yelp.de":
        print("yelp worker got non yelp url")

    response = None

    if tor:
        session = requests.session()
        session.proxies = {}
        session.proxies['http'] = "socks5h://localhost:9050"
        session.proxies['https'] = "'socks5h://localhost:9050'"
        try:
            response = session.get(url)
        except:
            print("error while requesting page with tor.")
    else:
        try:
            response = requests.get(url)
        except:
            print("error while requesting without tor")

    if response is None:
        return

    # TODO write the page object in the local database.
    return