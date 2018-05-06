# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json

from django.http import JsonResponse
from django.shortcuts import render
from django.db import transaction

from .models import OutLink


def get_outlinks(request):
    links = []
    try:
        with transaction.atomic():
            outlinks = OutLink.objects.filter(download_status=OutLink.DownloadStatus.Available)[:100]
            for link in outlinks:
                links.append(link.url)
                link.download_status = OutLink.DownloadStatus.Pending
                link.save()
            return JsonResponse({'links': links})
    except Exception as e:
        print e
        return JsonResponse({'links': []})
