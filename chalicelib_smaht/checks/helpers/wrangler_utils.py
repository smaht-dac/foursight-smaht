import json
from datetime import datetime
# from dcicutils import ff_utils
# from dcicutils.s3_utils import s3Utils
# from packaging import version


def item_has_property_with_value(item, property_name, property_value=None, compare_lists=False):
    """Check if item has a specific property and, if provided, a given value.
    - If property_value is None, just checks for existence of property.
    - If property_value is a list and item[property_name] is a string,
      return True if the string is in property_value.
    - If item[property_name] is a list and property_value is a string,
      return True if the string is in the list.
    - If both are lists, return True if they have any value in common.
    - If compare_lists is True, require both lists to match exactly (order-insensitive).
    """
    if property_value is None:
        return property_name in item

    val = item.get(property_name)
    if val is None:
        return False

    # Normalize to lists for comparison
    if not isinstance(property_value, list):
        property_value = [property_value]
    if not isinstance(val, list):
        val = [val]

    if compare_lists:
        # Require both lists to match elements (order-insensitive)
        return sorted(val) == sorted(property_value)

    # Check if there is any overlap
    return any(v in property_value for v in val)


def exclude_items_with_properties(items, properties={}):
    """Exclude items that have the given property/value info.
    properties: dict of property_name: property_value (or list of values)
    If property_value is None, exclude if property exists."""
    ok_items = []
    for item in items:
        if not any(
            item_has_property_with_value(item, prop, val)
            for prop, val in properties.items()
        ):
            ok_items.append(item)
    return ok_items


def include_items_with_properties(items, properties={}):
    """Include items that have the given property/value info.
    properties: dict of property_name: property_value (or list of values)
    If property_value is None, include if property exists."""
    ok_items = []
    for item in items:
        if any(
            item_has_property_with_value(item, prop, val)
            for prop, val in properties.items()
        ):
            ok_items.append(item)
    return ok_items
