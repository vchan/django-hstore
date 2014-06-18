from __future__ import unicode_literals, absolute_import

from django.db import models, connection
from django.utils.translation import ugettext_lazy as _
from django import get_version

from .descriptors import *
from .dict import *
from . import forms, utils


class HStoreField(models.Field):
    """ HStore Base Field """
    
    _dict_class = HStoreDict
    
    def _init_dict_class(self, value):
        return self._dict_class(value, self)

    def validate(self, value, *args):
        super(HStoreField, self).validate(value, *args)
        forms.validate_hstore(value)

    def contribute_to_class(self, cls, name):
        super(HStoreField, self).contribute_to_class(cls, name)
        setattr(cls, self.name, HStoreDescriptor(self))

    def get_default(self):
        """
        Returns the default value for this field.
        """
        if self.has_default():
            if callable(self.default):
                default = self.default()
            elif isinstance(self.default, dict):
                default = self.default
            else:
                return self.default
        else:
            if (not self.empty_strings_allowed or (self.null and not connection.features.interprets_empty_strings_as_nulls)):
                return None
            else:
                default = {}
        
        return self._init_dict_class(default)
        

    def get_prep_value(self, value):
        if isinstance(value, dict) and not isinstance(value, self._dict_class):
            return self._init_dict_class(value)
        else:
            return value

    def get_db_prep_value(self, value, connection, prepared=False):
        if not prepared:
            value = self.get_prep_value(value)
            if isinstance(value, self._dict_class):
                value.prepare(connection)
        return value

    def value_to_string(self, obj):
        return self._get_val_from_obj(obj)

    def db_type(self, connection=None):
        return 'hstore'

    def south_field_triple(self):
        from south.modelsinspector import introspector
        name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        args, kwargs = introspector(self)
        return name, args, kwargs


if get_version() >= '1.7':
    from .lookups import *

    HStoreField.register_lookup(HStoreGreaterThan)
    HStoreField.register_lookup(HStoreGreaterThanOrEqual)
    HStoreField.register_lookup(HStoreLessThan)
    HStoreField.register_lookup(HStoreLessThanOrEqual)
    HStoreField.register_lookup(HStoreContains)
    HStoreField.register_lookup(HStoreIContains)


class DictionaryField(HStoreField):
    description = _("A python dictionary in a postgresql hstore field.")

    def formfield(self, **kwargs):
        kwargs['form_class'] = forms.DictionaryField
        return super(DictionaryField, self).formfield(**kwargs)

    def _value_to_python(self, value):
        return value


class ModeledDictionaryField(DictionaryField):
    description = _("A python dictionary in a postgresql hstore field that preserves types.")
    
    _dict_class = HStoreModeledDictionary
    
    def _init_dict_class(self, value):
        return self._dict_class(value=value, field=self, schema=self.schema)
    
    def __init__(self, *args, **kwargs):
        self.schema = kwargs.pop('schema', None)
        # this is needed to ensure an exception is raised if the specified schema is not valid
        self._init_dict_class({})
        super(ModeledDictionaryField, self).__init__(*args, **kwargs)
    
    def get_default(self):
        # return an empty dict if method is being called as a static method
        # this case happens because django internals call this field before initializing it
        if not self.schema:
            return {}
        return super(ModeledDictionaryField, self).get_default()


class ReferencesField(HStoreField):
    description = _("A python dictionary of references to model instances in an hstore field.")

    def contribute_to_class(self, cls, name):
        super(ReferencesField, self).contribute_to_class(cls, name)
        setattr(cls, self.name, HStoreReferenceDescriptor(self))

    def formfield(self, **kwargs):
        kwargs['form_class'] = forms.ReferencesField
        return super(ReferencesField, self).formfield(**kwargs)

    def get_prep_lookup(self, lookup, value):
        if isinstance(value, dict):
            return utils.serialize_references(value)
        return value

    def get_prep_value(self, value):
        return utils.serialize_references(value)

    def to_python(self, value):
        return value if isinstance(value, dict) else HStoreReferenceDictionary({})

    def _value_to_python(self, value):
        return utils.acquire_reference(value)


class VirtualField(object):
    rel = None

    def contribute_to_class(self, cls, name):
        self.attname = self.name = name
        # cls._meta.add_virtual_field(self)
        get_field = cls._meta.get_field
        cls._meta.get_field = lambda name, many_to_many=True: self if name == self.name else get_field(name, many_to_many)
        models.signals.pre_init.connect(self.pre_init, sender=cls) #, weak=False)
        models.signals.post_init.connect(self.post_init, sender=cls) #, weak=False)
        setattr(cls, name, self)

    def pre_init(self, signal, sender, args, kwargs, **_kwargs):
        sender._meta._field_name_cache.append(self)

    def post_init(self, signal, sender, **kwargs):
        sender._meta._field_name_cache[:] = sender._meta._field_name_cache[:-1]

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self
        return instance.field1 + '/' + instance.field2

    def __set__(self, instance, value):
        if instance is None:
             raise AttributeError(u"%s must be accessed via instance" % self.related.opts.object_name)
        instance.field1, instance.field2 = value.split('/')

    def to_python(self, value):
        return value


# south compatibility
try:
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules(rules=[], patterns=['django_hstore\.hstore'])
except ImportError:
    pass
