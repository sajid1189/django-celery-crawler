# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from urlparse import urljoin, urlparse

from bs4 import BeautifulSoup


class Soup:
    def __init__(self, url, response):
        """
        A wrapper over BeautifulSoup class.
        :param content: is  response.content from a http request.
        """
        self.html = ""
        try:
            self.soup = BeautifulSoup(response.content, 'html.parser')
            self.html = response.text
            self.external_links = set()
            self.internal_links = set()
            self.absolute_internal_links = set()
            self.links = set()
            self.url = url
        except:
            self.soup = None

    def get_pretty_soup(self):
        if self.soup:
            return self.soup.prettify()
        else:
            return 'Sorry! The soup was nasty.'

    def get_all_p(self):
        if self.soup:
            return self.soup.find_all('p')

    def get_all_p_text(self):
        p_contents = []
        if self.soup:
            for p in self.get_all_p():
                p_contents.append(p.get_text())
        return p_contents

    def get_external_links(self):
        if not self.external_links:
            current_urlparsed_obj = urlparse(self.url)
            for link in self.get_all_links():

                urlparsed_obj = urlparse(link.get('href', ""))
                if current_urlparsed_obj.netloc != urlparsed_obj.netloc and urlparsed_obj.netloc != "":
                    self.external_links.add(link.get('href'))
        return self.external_links

    def get_internal_links(self):
        if not self.internal_links:
            current_urlparsed_obj = urlparse(self.url)

            for link in self.get_all_links():
                urlparsed_obj = urlparse(link.get("href", ""))
                if urlparsed_obj.netloc == "" or urlparsed_obj.netloc == current_urlparsed_obj.netloc :
                    self.internal_links.add(link.get('href'))
        return self.internal_links

    def get_all_links(self):
        if self.soup:
            if not self.links:
                self.links = set(self.soup.find_all('a', href=True))
        return self.links

    def get_absolute_internal_links(self):
        if not self.absolute_internal_links:
            for link in self.get_internal_links():
                if not self.is_media_resource(link):
                    self.absolute_internal_links.add(urljoin(self.url, link))
        return self.absolute_internal_links

    def is_media_resource(self, url):
        media_identifier_tokens = ['.jpg', '.png', 'jpeg', '.js', '.css', '.gif', '.pdf', '.doc', '.docx', '.svg', '.zip']
        for token in media_identifier_tokens:
            if url.lower().endswith(token):
                return True

        return False
