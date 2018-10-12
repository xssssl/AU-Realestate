from .models import soldhistory
from django.db.models import Sum, Count
from mymodels.soldhistoryModel.serializers import soldhistorySerializer, popsuburbSerializer
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
import datetime
from django.http import HttpResponse

# Create your views here.

# Return the sold history of a certain day
# URL Example: http://localhost:8000/api/soldhistory/date/2018/10/3/
@api_view(['GET',])
def soldhistory_bydate_view(request, yy, mm, dd, format=None):
    date = yy + '-' + mm + '-' + dd
    if request.method == 'GET':
        response = soldhistory.objects.values().filter(sold_date=date)
        serializer = soldhistorySerializer(response, many=True)
        return Response(serializer.data)

# Return the most popular suburbs
# URL Example: http://localhost:8000/popsuburbs/30
# @api_view(['GET',])
# def soldhistory_popsubs_view(request, period, format=None):
#     today = datetime.date.today()
#     date = (today - datetime.timedelta(days=int(period))).strftime('%Y-%m-%d')
#     if request.method == 'GET':
#         response = soldhistory.objects.values('suburb_id').annotate(total_num=Count('id')).filter(sold_date__gte=date).order_by('-total_num')[:15]
#         serializer = popsuburbSerializer(response, many=True)
#         return Response(serializer.data)

# Return the most popular suburbs
# URL Example: http://localhost:8000/popsuburbs/?period=30
@api_view(['GET',])
def soldhistory_popsubs_view(request):
    today = datetime.date.today()
    if request.method == 'GET':
        valid_period = ['14', '30', '60', '90', '180']
        period = request.query_params.get('period', '30')
        if period in valid_period:
            date = (today - datetime.timedelta(days=int(period))).strftime('%Y-%m-%d')
            response = soldhistory.objects.values('suburb_id','state','postcode','suburb').annotate(total_num=Count('id')).filter(sold_date__gte=date).order_by('-total_num')[:15]
            serializer = popsuburbSerializer(response, many=True)
            return Response(serializer.data)
            # return Response(period)
        else:
            return Response('Query is invalid.', status=status.HTTP_400_BAD_REQUEST)