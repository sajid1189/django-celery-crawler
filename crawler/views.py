# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json

from django.http import JsonResponse
from django.shortcuts import render
from django.db import transaction
from validators import length

from .models import OutLink


def get_outlinks(request):
    links = []
    result_size = 100
    len_param = request.GET.get('qs')
    print len_param
    if len_param and len_param > 0:
        result_size = len_param
    try:
        with transaction.atomic():
            outlinks = OutLink.objects.filter(download_status=OutLink.DownloadStatus.Available)
            if outlinks.count() < result_size:
                outlinks = outlinks[:result_size]
            for link in outlinks:
                links.append(link.url)
                link.download_status = OutLink.DownloadStatus.Pending
                link.save()
            return JsonResponse({'links': links})
    except Exception as e:
        print e
        return JsonResponse({'links': []})
