Variables can be passed to dbt commands using the vars parameter. Variables can be accessed in dbt code using `{{ var('variable_name') }}`.

Note: The vars parameter must be passed as a simple STRING with no special characters (i.e "\", "\n", etc). Do not pass in a dictionary object. 

Supported formats:
- Single variable (curly brackets optional): `"variable_name: value"`
- Multiple variables (curly brackets needed): `"{"key1": "value1", "key2": "value2"}"`
- Mixed types: `"{"string_var": "hello", "number_var": 42, "boolean_var": true}"`