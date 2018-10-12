from django.shortcuts import render
from .forms import analysisperiod
from urllib import request
# from rest_framework.parsers import JSONParser
import json

def get_popsubs_rlt(period):
    url = 'http://localhost:8000/api/soldhistory/popsuburbs/?format=json&period=%s' % period
    with request.urlopen(url) as f:
        resp = f.read().decode('utf-8')
        resp = json.loads(resp)
        for i in range(len(resp)):
            resp[i]['rank'] = i+1
    return resp

def popsuburbs(request):
    form = analysisperiod(initial={'Period': '30days'})
    ctx = {'form': form}
    if request.POST:
        form['Period'].initial = request.POST['Period']
        ctx['period'] = request.POST['Period'].strip('days')
        ctx['rlt'] = get_popsubs_rlt(request.POST['Period'].strip('days'))
    return render(request, "popsuburbs.html", ctx)
