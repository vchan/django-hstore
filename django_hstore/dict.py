try:
    import simplejson as json
except ImportError:
    import json

import pickle
from decimal import Decimal

from django.utils import six
from django.utils.encoding import force_text

from .compat import UnicodeMixin
from . import utils, exceptions


__all__ = [
    'HStoreDict',
    'HStoreReferenceDictionary',
    'HStoreModeledDictionary'
]


class HStoreDict(UnicodeMixin, dict):
    """
    A dictionary subclass which implements hstore support.
    """

    def __init__(self, value=None, field=None, instance=None, connection=None, **kwargs):
        # if passed value is string
        # ensure is json formatted
        if isinstance(value, six.string_types):
            try:
                value = json.loads(value)
            except ValueError as e:
                raise exceptions.HStoreDictException(
                    'HStoreDict accepts only valid json formatted strings.',
                    json_error_message=force_text(e)
                )
        elif value is None:
            value = {}

        # allow dictionaries only
        if not isinstance(value, dict):
            raise exceptions.HStoreDictException(
                'HStoreDict accepts only dictionary objects, None and json formatted string representations of json objects'
            )

        # ensure values are acceptable
        for key, val in value.items():
            value[key] = self.ensure_acceptable_value(val)

        super(HStoreDict, self).__init__(value, **kwargs)
        self.field = field
        self.instance = instance

        # attribute that make possible
        # to use django_hstore without a custom backend
        self.connection = connection

    def __setitem__(self, *args, **kwargs):
        args = (args[0], self.ensure_acceptable_value(args[1]))
        super(HStoreDict, self).__setitem__(*args, **kwargs)

    # This method is used both for python3 and python2
    # thanks to UnicodeMixin
    def __unicode__(self):
        if self:
            return force_text(json.dumps(self))
        return u''

    def __getstate__(self):
        if self.connection:
            d = dict(self.__dict__)
            d['connection'] = None
            return d
        return self.__dict__

    def __copy__(self):
        return self.__class__(self, self.field, self.connection)

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).iteritems():
            self[key] = value

    def ensure_acceptable_value(self, value):
        """
        - ensure booleans, integers, floats, Decimals, lists and dicts are
          converted to string
        - convert True and False objects to "true" and "false" so they can be
          decoded back with the json library if needed
        - convert lists and dictionaries to json formatted strings
        - leave alone all other objects because they might be representation of django schemas
        """
        if isinstance(value, bool):
            return force_text(value).lower()
        elif isinstance(value, (int, float, Decimal)):
            return force_text(value)
        elif isinstance(value, list) or isinstance(value, dict):
            return force_text(json.dumps(value))
        else:
            return value

    def prepare(self, connection):
        self.connection = connection

    def remove(self, keys):
        """
        Removes the specified keys from this dictionary.
        """
        queryset = self.instance._base_manager.get_query_set()
        queryset.filter(pk=self.instance.pk).hremove(self.field.name, keys)


class HStoreReferenceDictionary(HStoreDict):
    """
    A dictionary which adds support to storing references to models
    """
    def __getitem__(self, *args, **kwargs):
        value = super(self.__class__, self).__getitem__(*args, **kwargs)
        # if value is a string it needs to be converted to model instance
        if isinstance(value, six.string_types):
            reference = utils.acquire_reference(value)
            self.__setitem__(args[0], reference)
            return reference
        # otherwise just return the relation
        return value

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default


class HStoreModeledDictionary(HStoreDict):
    """
    A dictionary which adds support for types
    as long as keys are specified beforehand
    
    schema = {
        'key_name': {
            'type': <TypeClass>,
            'blank': True or False,
            'null': True or False,
            'default': 'default_value'
        }
    }
    """
    
    def __init__(self, value=None, field=None, instance=None, connection=None, schema=None, **kwargs):
        self.schema = self.validate_schema(schema)
        super(HStoreModeledDictionary, self).__init__(**kwargs)
    
    def __setitem__(self, *args, **kwargs):
        """
        check key name and value type before setting a key/value
        """
        key = self.ensure_acceptable_key(args[0])
        value = self.ensure_acceptable_value(key, args[1])
        args = (key, value)
        super(HStoreDict, self).__setitem__(*args, **kwargs)
    
    def __getitem__(self, *args, **kwargs):
        """
        get item or try returning default value for the specified key
        raises KeyError exception otherwise
        """
        try:
            value = super(HStoreModeledDictionary, self).__getitem__(*args, **kwargs)
            return pickle.loads(value)
        except KeyError as e:
            return self._get_default_for_key(args[0])
    
    def _get_default_for_key(self, key):
        """ returns the default value for the specified key """
        try:
            return self.schema[key].get('default', None)
        except KeyError:
            raise exceptions.HStoreModelException('%s is not a valid key' % key)
    
    def get(self, key, default=None):
        """ overwrite get method to support schema default value """
        try:
            return self.__getitem__(key)
        except KeyError:
            return self._get_default_for_key(key)
    
    def validate_schema(self, schema):
        """
        returns a validated schema, raises exception if validation fails
        """
        if not schema or not isinstance(schema, dict):
            raise exceptions.HStoreModelException('No valid schema specified for HStoreModeledDictionary')
            
        validated_schema = {}
        
        for key, options in schema.items():
            # if options is not a dictionary default to dict
            if isinstance(options, dict) is False:
                options = {}
            
            # if no type specified default to string
            if not options.get('type'):
                options['type'] = type(self.__str__())
            # if wrong type specified
            elif isinstance(options.get('type'), type) is not True:
                raise exceptions.HStoreModelException('type specified for key %s is not a valid type' % key)
            
            # blank defaults to False
            options['blank'] = options.get('blank', False)
            # null defaults to False
            options['null'] = options.get('null', False)            
            # if not specified, default value is None
            options['default'] = options.get('default', None)
            
            validated_schema[key] = options
        
        return validated_schema
    
    def ensure_acceptable_key(self, key):
        """
        ensure specified key is expected
        """
        if key not in self.schema.keys():
            raise exceptions.HStoreModelException('%s is not a valid key' % key)
        return key
    
    def ensure_acceptable_value(self, key, value):
        """
        ensure specified value is valid
        """
        if type(value) is not self.schema[key]['type']:
            raise exceptions.HStoreModelException(
                '%s is not a valid type for key %s, type %s expected' % (
                    value, key, self.schema[key]['type']
                )
            )
        # serialize data to mantain type
        value = pickle.dumps(value)
        return super(HStoreModeledDictionary, self).ensure_acceptable_value(value)
    
    def validate(self):
        """
        validates data according to its schema
        """
        pass
