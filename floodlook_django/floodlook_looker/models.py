from django.conf import settings
from django.db import models
from django.utils import timezone

# Create your models here.

class gauges(models.Model):
    gauge_id = models.CharField(max_length = 10)
    gauge_location = models.CharField(max_length = 50)
    gauge_tz = models.CharField(max_length = 50)
    gauge_pk = models.AutoField(primary_key = True)


class observations(models.Model):
    observation_stage = models.FloatField(null=True)
    observation_flow = models.FloatField(null=True)
    observation_gauge_id = models.ForeignKey(gauges,on_delete = models.CASCADE)
    observation_time = models.DateTimeField()
    observation_pk = models.AutoField(primary_key = True)


class forecast(models.Model):
    forecast_stage = models.FloatField(null=True)
    forecast_flow = models.FloatField(null=True)
    forecast_gauge_id = models.ForeignKey(gauges,on_delete = models.CASCADE)
    forecast_time_added = models.DateTimeField(default = timezone.now)
    forecast_time = models.DateTimeField()
    forecast_pk = models.AutoField(primary_key = True)

