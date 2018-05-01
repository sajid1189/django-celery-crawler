import hashlib
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
    from crawler.models import OutLink, Page, Domain, get_url_hash
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
        print unicode(e)
        return
    if not outlink_obj.download_status:
        try:
            Page.objects.get(url_hash=get_url_hash(url))
            outlink_obj.download_status = True
            outlink_obj.save()
            print 'outlink download status changed'
            return
        except Page.DoesNotExist:

            try:
                session = requests.session()
                session.proxies = {}
                session.proxies['http'] = 'socks5h://localhost:9050'
                session.proxies['https'] = 'socks5h://localhost:9050'

                response = session.get(url)
                if response.status_code == 200:
                    soup = Soup(url, response)
                    if soup.soup:
                        outlink_obj.download_status = True
                        outlink_obj.save()
                        Page.objects.create(url=url, content=soup.html)
                    for outlink in soup.get_absolute_internal_links():
                        if outlink.startswith("mailto") or outlink.startswith("tel:") or outlink.startswith("javascript:") or "#" in outlink:
                            continue
                        elif '#' in outlink:
                            outlink = outlink.split('#')[0]
                        try:
                            url_hash = get_url_hash(outlink)
                            OutLink.objects.get(url_hash=url_hash)

                        except OutLink.DoesNotExist:
                            outlink = OutLink.objects.create(url=outlink)
                            print 'created {}'.format(outlink)

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

        except:
            print "something went wrong in Page lookup or creation."

