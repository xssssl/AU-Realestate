"""Website URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""

from django.conf.urls import url
from django.contrib import admin
from rest_framework.urlpatterns import format_suffix_patterns
from . import popsuburbs
from . import testdb
from mymodels.soldhistoryModel import views as soldhistoryModel_views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^popsuburbs/$', popsuburbs.popsuburbs),
    # url(r'^testmydb/$', testdb.testdb),
    url(r'^api/soldhistory/date/(?P<yy>20[0-1]{1}[0-9]{1}|19[7-9]{1}[0-9]{1})/(?P<mm>[1-9]{1}|1[0-2]{1})/(?P<dd>[1-9]{1}|[1-2]{1}[0-9]{1}|3[0-1]{1})/$', soldhistoryModel_views.soldhistory_bydate_view),
    url(r'^api/soldhistory/popsuburbs/$', soldhistoryModel_views.soldhistory_popsubs_view),
]

urlpatterns = format_suffix_patterns(urlpatterns)