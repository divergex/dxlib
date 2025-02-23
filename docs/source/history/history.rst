.. _history:

History Class Documentation
===========================

Class Overview
--------------

The `History` class is a container for dense mutable time-series data, allowing easy manipulation, retrieval, and analysis. It supports indexing, column management, data merging, and operations on stored datasets.

Usage
-----

To use the `History` class, initialize it with a schema and data:

.. code-block:: python

   from dxlib.history import History, HistorySchema
   import pandas as pd

   schema = HistorySchema(index={"date": "datetime64[ns]"}, columns={"close": "float64"})
   data = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "close": [100.5, 101.0]})
   history = History(history_schema=schema, data=data)

Methods
-------

Index Manipulation
~~~~~~~~~~~~~~~~~~

- **idx(name)**: Returns the level of the index by name.
- **iidx(idx)**: Returns the name of the index at a given level.
- **levels(names=None, to_list=True)**: Retrieves index levels.
- **index(name)**: Gets the index by name.
- **indices (property)**: Lists available indices.

Data Manipulation
~~~~~~~~~~~~~~~~~

- **concat(other, keep="first")**: Merges another `History` instance, removing duplicates.
- **extend(other)**: Extends history columns from another instance.
- **get(index=None, columns=None, raw=False)**: Extracts a subset of history data.
- **set(values)**: Updates specific values in the dataset.
- **dropna()**: Returns a new history instance with NaN values removed.

Operations
~~~~~~~~~~

- **apply(func, *args, **kwargs)**: Applies a function to the data.
- **op(other, columns, other_columns, operation)**: Applies an operation on matching indices of self and another history.

Serialization
~~~~~~~~~~~~~

- **to_dict()**: Converts the history to a dictionary.
- **from_dict(data)**: Creates a `History` instance from a dictionary.
- **store(storage_path, key)**: Saves the history to a specified storage location.
- **load(storage_path, key)**: Loads a history instance from storage.
- **cache_exists(cache_path, key)**: Checks if a history cache exists.

Special Methods
~~~~~~~~~~~~~~~

- **__len__()**: Returns the number of records.
- **__getitem__(key)**: Retrieves a specific column.
- **__str__()**: Provides a string representation of the history instance.


Additional Information
----------------------

For more details, refer to the `HistorySchema` class, which defines the structure and metadata of the stored data.


Class Autodocs
--------------

.. automodule:: dxlib.history.history
   :members:
   :undoc-members:
   :show-inheritance:
