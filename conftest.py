import collections
from collections.abc import Mapping, MutableMapping, Sequence

if not hasattr(collections, "MutableMapping"):
    collections.MutableMapping = MutableMapping
if not hasattr(collections, "Mapping"):
    collections.Mapping = Mapping
if not hasattr(collections, "Sequence"):
    collections.Sequence = Sequence
