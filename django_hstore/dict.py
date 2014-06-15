try:
    import simplejson as json
except ImportError:
    import json

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

    def __init__(self, value=None, field=None, instance=None, connection=None, **params):
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

        super(HStoreDict, self).__init__(value, **params)
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
        - leave alone all other objects because they might be representation of django models
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
    
    model = {
        'key_name': {
            'type': <TypeClass>,
            'blank': True or False,
            'default': 'default_value'
        }
    }
    """
    
    def __init__(self, model=None, value=None, field=None, instance=None, connection=None, **params):
        self.model = self.validate_model(model)
        super(HStoreModeledDictionary, self).__init__(**params)
    
    def validate_model(self, model):
        """
        returns a validated method, raise exception if validation fails
        """
        if not model:
            raise exceptions.HStoreModelException('No valid model specified for HStoreModeledDictionary')
            
        validated_model = {}
        
        for key, options in model.items():
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
            
            # if not specified, default value is None
            options['default'] = options.get('default', None)
            
            validated_model[key] = options
        
        return validated_model
