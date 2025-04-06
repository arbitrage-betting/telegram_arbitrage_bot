import orjson
from typing import Any, List, Dict

def read_orjson(file_path: str) -> Any:
    """
    Reads and returns the contents of a JSON file using orjson.
    Args:
    file_path (str): The path to the JSON file to be read.
    Returns:
    Any: The contents of the JSON file.
    """
    with open(file_path, 'rb') as file:
        return orjson.loads(file.read())

def safe_read_orjson(file_path: str) -> List:
    """
    Reads and returns the contents of a JSON file using orjson. If not found use empty list.
    Use mostly on flask app.
    Args:
    file_path (str): The path to the JSON file to be read.
    Returns:
    Any: The contents of the JSON file.
    """
    try: 
        with open(file_path, 'rb') as file: 
            return orjson.loads(file.read())
    except (FileNotFoundError, orjson.JSONDecodeError) as e:
        return []

def write_orjson(file_path: str, data: Any) -> None:
    """
    Writes data to a JSON file using orjson.
    Args:
    file_path (str): The path to the JSON file to be written.
    data (Any): The data to write to the JSON file.
    Returns:
    None
    """
    with open(file_path, 'wb') as file:
        file.write(orjson.dumps(data))