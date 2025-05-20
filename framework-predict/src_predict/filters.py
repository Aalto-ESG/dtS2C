def categorize_columns(columns: list[str]) -> dict[str, list[str]]:
    """
    Categorizes column names into six main categories:
    1. CPU-related metrics
    2. Memory-related metrics
    3. Disk I/O metrics
    4. Network metrics
    5. Kepler-related metrics
    6. Other metrics
    7. All of the above metrics (combined)

    Args:
        columns (list): List of column names.

    Returns:
        dict: Dictionary with categories as keys and lists of column names under each category.
    """
    import json

    # Initialize categories
    categories: dict[str, list[str]] = {
        "CPU": [],
        "Memory": [],
        "Disk_io": [],
        "Network": [],
        "Kepler": [],
        "Other": [],
        "All": []
    }

    # Define keywords to classify metrics
    cpu_keywords = ["cpu", "schedstat", "softnet", "pressure_cpu", "load"]
    memory_keywords = ["memory", "vmstat", "pgfault"]
    disk_keywords = ["disk", "io_time", "write", "flush", "read"]
    network_keywords = ["network", "netstat", "receive", "transmit", "multicast"]
    kepler_keywords = ["kepler", "joules", "power"]

    for col in columns:
        try:
            # Try parsing the column name as JSON if possible
            parsed_col = json.loads(col)
            name = parsed_col.get("__name__", "")
        except json.JSONDecodeError:
            # Fallback to plain column name if not a JSON string
            name = col

        # Add all cols to the combined category
        categories["All"].append(col)

        # Check categorization based on keywords
        if any(keyword in name.lower() for keyword in cpu_keywords):
            categories["CPU"].append(col)
        elif any(keyword in name.lower() for keyword in memory_keywords):
            categories["Memory"].append(col)
        elif any(keyword in name.lower() for keyword in disk_keywords):
            categories["Disk_io"].append(col)
        elif any(keyword in name.lower() for keyword in network_keywords):
            categories["Network"].append(col)
        elif any(keyword in name.lower() for keyword in kepler_keywords):
            categories["Kepler"].append(col)
        else:
            categories["Other"].append(col)

    return categories
