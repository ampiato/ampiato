from __future__ import annotations

from django.db import models


class BlokDef(models.Model):
    Jmeno = models.TextField()
    Barva = models.TextField()

    class Meta:
        db_table = "BlokDef"


class BlokVykon(models.Model):
    IdBlokDef = models.ForeignKey(BlokDef, db_column="IdBlokDef", on_delete=models.CASCADE)
    Time = models.DateTimeField()
    pInst = models.FloatField()
    pDos = models.FloatField()
    pMin = models.FloatField()

    class Meta:
        db_table = "BlokVykon"
        unique_together = ["IdBlokDef", "Time"]


class BlokVS(models.Model):
    IdBlokDef = models.ForeignKey(BlokDef, db_column="IdBlokDef", on_delete=models.CASCADE)
    Time = models.DateTimeField()
    Abs = models.FloatField()

    class Meta:
        db_table = "BlokVS"
        unique_together = ["IdBlokDef", "Time"]


class Market(models.Model):
    Time = models.DateTimeField()
    CzkEur = models.FloatField()
    cEle = models.FloatField()

    class Meta:
        db_table = "Market"
        unique_together = ["Time"]

