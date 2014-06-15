from django.db import models
from .dict import *


__all__ = [
    'HStoreDescriptor',
    'HStoreReferenceDescriptor'
]


class HStoreDescriptor(models.fields.subclassing.Creator):
    def __set__(self, obj, value):
        value = self.field.to_python(value)
        if isinstance(value, dict):
            value = HStoreDict(
                value=value, field=self.field, instance=obj
            )
        obj.__dict__[self.field.name] = value


class HStoreReferenceDescriptor(models.fields.subclassing.Creator):
    def __set__(self, obj, value):
        value = self.field.to_python(value)
        if isinstance(value, dict):
            value = HStoreReferenceDictionary(
                value=value, field=self.field, instance=obj
            )
        obj.__dict__[self.field.name] = value
