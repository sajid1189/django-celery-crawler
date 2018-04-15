import os
from urlparse import urlparse
from celery import Celery

import requests

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


@app.task
def worker(url):
    from crawler.models import OutLink, Page, Domain
    outlink_obj, created = OutLink.objects.get_or_create(url=url)
    domain = urlparse(url).netloc
    if not outlink_obj.download_status:
        try:
            Page.objects.get(url=url)
        except Page.DoesNotExist:
            soup = Soup(url)

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    soup = Soup(response)
            except requests.exceptions.Timeout:
                domain, created = Domain.objects.get_or_create(domain=domain)
                domain.set_timeout()
                outlink, created = OutLink.objects.get_or_create(url=url)
                outlink.set_timeout()
            except:
                pass
            if soup.soup:
                outlink_obj.download_status = True
                outlink_obj.save()
                Page.objects.create(url=url, content=soup.html)
            for link in soup.get_absolute_internal_links():
                try:
                    outlink = OutLink.objects.create(url=link)
                except:
                    pass

