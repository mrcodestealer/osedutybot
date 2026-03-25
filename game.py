# game.py
import random
from datetime import datetime, timedelta

_active_games = {}  # user_id -> { 'number': str, 'expiry': datetime, 'job_id': str }

def _generate_number(digits=8):
    return ''.join(str(random.randint(0, 9)) for _ in range(digits))

def start_game(user_id):
    """Start a new game and return the number to remember."""
    number = _generate_number()
    expiry = datetime.now() + timedelta(seconds=15)
    _active_games[user_id] = {
        'number': number,
        'expiry': expiry,
        'job_id': None
    }
    return number

def set_game_job(user_id, job_id):
    """Store the scheduler job ID so it can be cancelled later."""
    if user_id in _active_games:
        _active_games[user_id]['job_id'] = job_id

def get_game_job(user_id):
    """Retrieve the job ID for the user's active game."""
    game = _active_games.get(user_id)
    return game.get('job_id') if game else None

def check_answer(user_id, answer_text):
    """
    Verify the answer. Returns (response_message, should_clear, job_id).
    If no game exists, returns (None, False, None).
    """
    game = _active_games.get(user_id)
    if not game:
        return None, False, None

    job_id = game.get('job_id')
    number = game['number']

    # Timeout check
    if datetime.now() > game['expiry']:
        del _active_games[user_id]
        return f"⏰ Time's up! The number was **{number}**.", True, job_id

    # Compare answers
    if answer_text.strip() == number:
        del _active_games[user_id]
        return "✅ Correct! Well done!", True, job_id
    else:
        del _active_games[user_id]
        return f"❌ Wrong! The number was **{number}**.", True, job_id

def has_active_game(user_id):
    return user_id in _active_games

def clear_game(user_id):
    if user_id in _active_games:
        del _active_games[user_id]