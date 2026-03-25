import re
from datetime import datetime, timedelta
import functools

def parse_duration(duration_str):
    pattern = re.compile(r'^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$', re.IGNORECASE)
    match = pattern.match(duration_str.strip())
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}. Use e.g., 1h30m, 45s, 2h5s")
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    total = hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise ValueError("Duration must be positive")
    return total

def parse_absolute_time(time_str):
    """Convert a time string like '8:39PM', '2039', '8pm' into a datetime (today or tomorrow)."""
    time_str = time_str.strip().lower()
    now = datetime.now()
    today = now.date()

    # Pattern 1: HH:MMam/pm
    match = re.match(r'^(\d{1,2}):(\d{2})\s*(am|pm)$', time_str)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        meridian = match.group(3)
        if meridian == 'pm' and hour != 12:
            hour += 12
        elif meridian == 'am' and hour == 12:
            hour = 0
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    # Pattern 2: HHam/pm (no colon)
    match = re.match(r'^(\d{1,2})\s*(am|pm)$', time_str)
    if match:
        hour = int(match.group(1))
        minute = 0
        meridian = match.group(2)
        if meridian == 'pm' and hour != 12:
            hour += 12
        elif meridian == 'am' and hour == 12:
            hour = 0
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    # Pattern 3: HHMM (24-hour)
    if re.match(r'^\d{4}$', time_str):
        hour = int(time_str[:2])
        minute = int(time_str[2:])
        if hour > 23 or minute > 59:
            raise ValueError("Invalid time: hours must be 00-23, minutes 00-59")
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    # Pattern 4: HH:MM (24-hour)
    match = re.match(r'^(\d{1,2}):(\d{2})$', time_str)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        if hour > 23 or minute > 59:
            raise ValueError("Invalid time: hours must be 00-23, minutes 00-59")
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    raise ValueError(f"Unsupported time format: {time_str}. Use e.g., 8:39PM, 2039, 8pm, 20:39")

def schedule_reminder(chat_id, user_id, duration_str, message, scheduler, send_func):
    try:
        delay_seconds = parse_duration(duration_str)
    except ValueError as e:
        return str(e)

    run_time = datetime.now() + timedelta(seconds=delay_seconds)
    reminder_text = f'<at user_id="{user_id}">you</at> ⏰ Reminder: {message}'
    scheduler.add_job(func=send_func, trigger='date', run_date=run_time, args=[chat_id, reminder_text])

    # Format readable duration
    hours, remainder = divmod(delay_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    duration_readable = ''.join(parts)

    return f"✅ Reminder set for {duration_readable} from now. I'll remind you about: {message}"

def schedule_reminder_absolute(chat_id, user_id, time_str, message, scheduler, send_func):
    try:
        run_time = parse_absolute_time(time_str)
    except ValueError as e:
        return str(e)

    reminder_text = f'<at user_id="{user_id}">you</at> ⏰ Reminder: {message}'
    scheduler.add_job(func=send_func, trigger='date', run_date=run_time, args=[chat_id, reminder_text])

    # Format the time for user feedback (e.g., 08:39 PM)
    time_str_display = run_time.strftime("%I:%M %p").lstrip('0')
    return f"✅ Reminder set for {time_str_display}. I'll remind you about: {message}"