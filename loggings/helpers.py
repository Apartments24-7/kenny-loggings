from .models import LogExtra


def normalize_extras(obj, extras=None, manual_extras=None):
    """
    obj - the instance to lookup attributes on
    extras - a list of attributes, using __ for chaining
    manual_extras - a list of tuples (field, val)
    """
    resolved_extras = []

    # Resolve attribute names into tuple(key, val)
    for extra in set(extras or []):
        val = obj
        for field_name in extra.split("__"):
            val = getattr(val, field_name)
        resolved_extras.append((field_name, val))

    # Treat extras/manual extras the same
    combined_extras = resolved_extras + (manual_extras or [])
    unique_extras = {}

    # Prevent dupe key/val sets
    for key, val in combined_extras:
        unique_extras[f"{key} {val}"] = (key, val)

    return tuple(unique_extras.values())


def create_extra(log_id, field_name, value):
    # Avoid duplicate extras
    log, _ = LogExtra.objects.get_or_create(
        log_id=log_id,
        field_name=field_name,
        field_value=value
    )
    return log
