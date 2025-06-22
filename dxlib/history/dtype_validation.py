import pandas as pd
import pandas.api.types as ptypes
import numbers


type_validators = {
    int:     lambda s: ptypes.is_integer_dtype(s),
    float:   lambda s: ptypes.is_float_dtype(s),
    str:     lambda s: ptypes.is_string_dtype(s),
    bool:    lambda s: ptypes.is_bool_dtype(s),
    numbers.Number: lambda s: ptypes.is_numeric_dtype(s),
    pd.Timestamp: lambda s: ptypes.is_datetime64_any_dtype(s),
    pd.DatetimeTZDtype: lambda s: ptypes.is_datetime64_any_dtype(s),
}

def validate_series_dtype(series, expected_type):
    validator = type_validators.get(expected_type)
    return (validator and validator(series)) or series.apply(lambda x: isinstance(x, expected_type)).all()
