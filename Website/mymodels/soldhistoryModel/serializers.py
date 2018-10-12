from rest_framework import serializers
from mymodels.soldhistoryModel.models import soldhistory

# Replaced by the ModelSerializer below
# class soldhistorySerializer(serializers.Serializer):
#     id = serializers.IntegerField(required=False, read_only=True)
#     state = serializers.CharField(max_length=4, required=False, )
#     postcode = serializers.CharField(max_length=5, required=False, )
#     suburb = serializers.CharField(max_length=100, required=False, )
#     suburb_id = serializers.IntegerField(required=False)
#     full_address = serializers.CharField(required=False, max_length=255, allow_blank=True)
#     property_type = serializers.CharField(required=False, max_length=255, allow_blank=True)
#     price = serializers.DecimalField(required=False, max_digits=10, decimal_places=2)
#     bedrooms = serializers.IntegerField(required=False)
#     bathrooms = serializers.IntegerField(required=False)
#     carspaces = serializers.IntegerField(required=False)
#     sold_date = serializers.DateField(required=False)
#     agent = serializers.CharField(required=False, max_length=255, allow_blank=True)
#     url = serializers.CharField(required=False, max_length=255, allow_blank=True)
#     update_date = serializers.DateField(required=False)
#     processed_date = serializers.DateField(required=False)

class soldhistorySerializer(serializers.ModelSerializer):
    class Meta:
        model = soldhistory
        fields = '__all__'

class popsuburbSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=False, read_only=True)
    state = serializers.CharField(max_length=4, required=False, )
    postcode = serializers.CharField(max_length=5, required=False, )
    suburb = serializers.CharField(max_length=100, required=False, )
    suburb_id = serializers.IntegerField(required=False)
    full_address = serializers.CharField(required=False, max_length=255, allow_blank=True)
    property_type = serializers.CharField(required=False, max_length=255, allow_blank=True)
    price = serializers.DecimalField(required=False, max_digits=10, decimal_places=2)
    bedrooms = serializers.IntegerField(required=False)
    bathrooms = serializers.IntegerField(required=False)
    carspaces = serializers.IntegerField(required=False)
    sold_date = serializers.DateField(required=False)
    agent = serializers.CharField(required=False, max_length=255, allow_blank=True)
    url = serializers.CharField(required=False, max_length=255, allow_blank=True)
    update_date = serializers.DateField(required=False)
    processed_date = serializers.DateField(required=False)
    total_num = serializers.IntegerField(required=False)