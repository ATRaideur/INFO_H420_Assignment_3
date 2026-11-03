
def validate_metadata(data):
    if not isinstance(data, dict):
        raise ValueError("Metadata must be a JSON object.")

    for field in ("title", "summary", "category"):
        if not isinstance(data.get(field), str):
            raise ValueError(f"'{field}' must be a string.")

    keywords = data.get("keywords")

    if not isinstance(keywords, list):
        raise ValueError("'keywords' must be a list.")

    if not all(isinstance(keyword, str) for keyword in keywords):
        raise ValueError("Every keyword must be a string.")

    return data


