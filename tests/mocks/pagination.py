def page_response(value: dict) -> dict:
    count = len(value["items"])
    return {
        "pageNum": 1,
        "pageSize": 50,
        "totalItems": count,
        "totalPages": 1 if count else 0,
        **value,
    }
