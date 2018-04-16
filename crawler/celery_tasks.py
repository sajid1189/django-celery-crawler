import os
from urlparse import urlparse
from celery import Celery

from django.utils import timezone
from django.conf import settings

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
    if url.startswith("mailto"):
        return
    # ################### CHECK DOMAIN TIMEOUT  ########################
    domain_string = urlparse(url).netloc
    try:
        domain, created = Domain.objects.get_or_create(domain=domain_string)
        if not created and domain.timeout:
            if domain.last_attempt:
                if domain.last_attempt + timezone.timedelta(seconds=settings.REQUEST_DOMAIN_FALLBACK_TIME) < timezone.now():
                    print('still in timeout')
                    return
    except Exception as e:
        print "something went wrong at: {}".format(url)
        return
    if not outlink_obj.download_status:
        print "outlink obj status false"

        try:
            Page.objects.get(url=url)
            outlink_obj.download_status = True
            outlink_obj.save()
            print 'outlink download status changed'
            return
        except Page.DoesNotExist:

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    soup = Soup(url, response)
                    if soup.soup:
                        outlink_obj.download_status = True
                        outlink_obj.save()
                        Page.objects.create(url=url, content=soup.html)
                    for link in soup.get_absolute_internal_links():
                        if link.startswith("mailto") or link.startswith("tel:") or link.startswith("javascript:"):
                            continue
                        try:
                            outlink = OutLink.objects.create(url=link)
                            print 'created {}'.format(outlink)
                        except:
                            print 'outlink was not created'
                            pass
            except requests.exceptions.Timeout:
                print 'request timeout'
                domain.set_timeout()
                outlink_obj.set_timeout()
                if not created:
                    outlink_obj.download_status = False
                    outlink_obj.save()
                    return

