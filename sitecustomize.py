import collections
from collections.abc import MutableMapping

# Provide backwards compatibility for libraries expecting MutableMapping in collections
if not hasattr(collections, "MutableMapping"):
    collections.MutableMapping = MutableMapping
