# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import hashlib
from django.db import models
from django.utils import timezone
# Create your models here.


class Page(models.Model):
    url = models.TextField()
    content = models.TextField()
    depth = models.IntegerField(null=True, blank=True)
    url_hash = models.CharField(max_length=34, unique=True)
    content_hash = models.CharField(max_length=34)
    created_at = models.DateTimeField(auto_now_add=True)

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        if not self.pk:
            self.url_hash = hashlib.md5(self.url.encode('utf-8')).hexdigest()
            self.content_hash = hashlib.md5(self.content.encode('utf-8')).hexdigest()
        super(Page, self).save(force_insert=force_insert,
                               force_update=force_update,
                               using=using,
                               update_fields=update_fields
                               )


class OutLink(models.Model):
    url = models.TextField()
    url_hash = models.CharField(max_length=34, unique=True)
    download_status = models.BooleanField(default=False, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    timeout = models.BooleanField(default=False)
    last_attempt = models.DateTimeField(null=True)

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        if not self.pk:
            self.url_hash = hashlib.md5(self.url.encode('utf-8')).hexdigest()
        super(OutLink, self).save(force_insert=force_insert,
                                  force_update=force_update,
                                  using=using,
                                  update_fields=update_fields
                                  )

    def set_timeout(self):
        self.timeout = True
        self.last_attempt = timezone.now()
        self.save()

    def __str__(self):
        return self.url


class Domain(models.Model):
    domain = models.CharField(max_length=255)
    timeout = models.BooleanField(default=False)
    last_attempt = models.DateTimeField(null=True)

    def __str__(self):
        return self.domain

    def set_timeout(self):
        self.timeout = True
        self.last_attempt = timezone.now()
        self.save()

