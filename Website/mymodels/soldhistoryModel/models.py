from django.db import models

# Create your mymodels here.

class soldhistory(models.Model):
    id = models.AutoField(max_length=11, primary_key=True)
    state = models.CharField(max_length=4)
    postcode = models.CharField(max_length=5)
    suburb = models.CharField(max_length=100)
    suburb_id = models.IntegerField(blank=True)
    full_address = models.CharField(max_length=255, blank=True)
    property_type = models.CharField(max_length=255, blank=True)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    bedrooms = models.IntegerField(blank=True)
    bathrooms = models.IntegerField(blank=True)
    carspaces = models.IntegerField(blank=True)
    sold_date = models.DateField(blank=True)
    agent = models.CharField(max_length=255, blank=True)
    url = models.CharField(max_length=255, blank=True)
    update_date = models.DateField(auto_now_add=True, blank=True)
    processed_date = models.DateField(blank=True)

    class Meta:
        managed = False     # no database table creation or deletion operations will be performed for this model
        db_table = 'overall_sold_history_increment'     # to specify the name of the table

