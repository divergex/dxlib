.. _history_schema:

HistorySchema Documentation
===========================

The ``HistorySchema`` class defines the structure of a dataset, including its index and column names,
each mapped to their respective data types. This schema is often used in data storage, retrieval, and
serialization processes.

Class Overview
--------------

.. code-block:: python

    class HistorySchema(Serializable, metaclass=RegistryBase):
        """
        A schema is the structure of a data set.
        It contains the index names mapped to their respective types and levels,
        as well as the column names mapped to their types.
        """



Usage
-----

Creating a Schema
~~~~~~~~~~~~~~~~~

You can create a ``HistorySchema`` object by specifying the ``index`` and ``columns``:

.. code-block:: python

    schema = HistorySchema(
        index={"security": str, "date": pd.Timestamp},
        columns={"open": float, "close": float}
    )

Accessing Schema Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``index_names``: A list of the index names.

  .. code-block:: python

        schema.index_names  # ['security', 'date']

- ``column_names``: A list of the column names.

  .. code-block:: python

        schema.column_names  # ['open', 'close']

Copying a Schema
~~~~~~~~~~~~~~~~

You can create a copy of the schema:

.. code-block:: python

    schema_copy = schema.copy()

Checking Index and Column Membership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can check if an index or column name exists in the schema:

.. code-block:: python

    schema.in_index('security')  # True
    schema.in_column('open')     # True


Attributes
----------

- ``index (Dict[str, Type])``: A dictionary where keys are index names (e.g., ``security``, ``date``)
  and values are the respective types (e.g., ``str``, ``Timestamp``).
- ``columns (Dict[str, Type])``: A dictionary where keys are column names (e.g., ``open``, ``close``)
  and values are the respective types (e.g., ``float``).

Serialization
-------------

``HistorySchema`` supports serialization to and from dictionaries and JSON.

Converting to a Dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~

You can convert the schema to a dictionary representation:

.. code-block:: python

    schema_dict = schema.to_dict()
    # Output: {'index': {'security': 'str', 'date': 'Timestamp'}, 'columns': {'open': 'float', 'close': 'float'}}

Deserializing from a Dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can reconstruct the schema from a dictionary:

.. code-block:: python

    schema_from_dict = HistorySchema.from_dict(schema_dict)

Converting to JSON
~~~~~~~~~~~~~~~~~~

You can serialize the schema to a JSON string:

.. code-block:: python

    schema_json = schema.__json__()
    # Output: '{"index": {"security": "str", "date": "Timestamp"}, "columns": {"open": "float", "close": "float"}}'

Saving and Loading from File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The schema can be saved to a file and loaded back.

- **Save to File**:

.. code-block:: python

    schema.store('/path/to/save', 'schema_key')

- **Load from File**:

.. code-block:: python

    loaded_schema = HistorySchema.load('/path/to/save', 'schema_key')


Class Autodoc
-------------

.. autoclass:: dxlib.history.HistorySchema
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: __init__, __str__, __repr__, __eq__, __ne__, __json__, to_dict, from_dict, store, load
    :inherited-members:
