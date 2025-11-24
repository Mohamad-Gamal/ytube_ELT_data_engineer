from datetime import datetime, timedelta    

def parse_duration(duration_str):
    """
    Parse a duration string formatted as 'HH:MM:SS' into total seconds.

    Args:
        duration_str (str): Duration string in the format 'HH:MM:SS'.

    Returns:
        int: Total duration in seconds.
    """
    duration_str = duration_str.strip().replace('P', ''). replace('T', '')
    components = ['D', 'H', 'M', 'S']
    values = {'D': 0, 'H': 0, 'M': 0, 'S': 0}
    for comp in components:
        if comp in duration_str:
            value, duration_str = duration_str.split(comp)
            values[comp] = int(value)

    total_duration = timedelta(
                                days=values['D'], hours=values['H'], 
                                minutes=values['M'], seconds=values['S'])
    return total_duration

def transform_data(row_data):

    duration_dt = parse_duration(row_data["duration"])
    row_data['duration'] = (datetime.min + duration_dt).time()
    row_data['video_type'] = 'short' if duration_dt.total_seconds() < 60 else 'normal'