def flatten_json(json_obj:dict, parent_key='', sep='_'):
    flat_dict = {}
    for key, value in json_obj.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                flat_dict.update(flatten_json({f"{new_key}[{i}]": item}, '', sep=sep))
        else:
            flat_dict[new_key] = value
    return flat_dict
