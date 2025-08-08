import os
from twikit import Client
from twikit import errors as twikit_errors
import asyncio
from dotenv import load_dotenv
import json
import random
import time
from datetime import datetime
import pytz
from data_jobs import COOKIES_FILE, CONFIG_FILE, FOLLOWING_FILE, TWEETS_FILE, LOGGING_FILE

load_dotenv()
USERNAME = os.getenv('TWITTER_USERNAME', '')
EMAIL = os.getenv('TWITTER_EMAIL', '')
PASSWORD = os.getenv('TWITTER_PASSWORD', '')
TOTP_SECRET = os.getenv('TWITTER_TOTP_SECRET', '')
client = Client('en-US')

session_log = {
    'session_id': None,
    'start_time': None,
    'errors': [],
    'calls': 0,
    'new_following_count': 0,
    'tweets_collected': 0,
    'attempts': 0
}


def _atomic_write_json(path: str, data) -> None:
    """Write JSON atomically by writing to a temp file then replacing."""
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def handle_errors(default_return=None, function_name=None):
    """Decorator to handle common Twitter API errors"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            error_map = {
                twikit_errors.UserNotFound: ('user_not_found', "User not found"),
                twikit_errors.UserUnavailable: ('user_unavailable', "User unavailable"),
                twikit_errors.Forbidden: ('access_forbidden', "Access forbidden"),
                twikit_errors.Unauthorized: ('unauthorized', "Not authorized"),
                twikit_errors.AccountSuspended: ('account_suspended', "Account suspended"),
                twikit_errors.TooManyRequests: ('rate_limited', "Rate limited"),
                twikit_errors.ServerError: ('server_error', "Server error"),
                twikit_errors.BadRequest: ('bad_request', "Bad request"),
            }

            try:
                return await func(*args, **kwargs)
            except tuple(error_map.keys()) as e:
                error_type, message = error_map[type(e)]
                print(f"Error: {message}")
                await log_errors(error_type, str(e), function_name or func.__name__)
                return default_return if default_return is not None else []
            except Exception as e:
                print(f"Error: Unexpected error: {e}")
                await log_errors('unexpected_error', str(e), function_name or func.__name__)
                return default_return if default_return is not None else []

        return wrapper
    return decorator


@handle_errors(default_return=[])
async def log_session_data(status, additional_data=None):
    """Log session data to logging.json"""
    cst = pytz.timezone('US/Central')
    current_time = datetime.now(cst).isoformat()
    log_entry = {
        'session_id': session_log['session_id'],
        'timestamp': current_time,
        'status': status,
        'start_time': session_log['start_time'],
        'runtime_seconds': (datetime.now(cst) - datetime.fromisoformat(session_log['start_time'])).total_seconds() if session_log['start_time'] else 0,
        'errors': session_log['errors'],
        'calls': session_log['calls'],
        'new_following_count': session_log['new_following_count'],
        'tweets_collected': session_log['tweets_collected'],
        'attempts': session_log['attempts']
    }

    if additional_data:
        log_entry.update(additional_data)
    try:
        with open(LOGGING_FILE, 'r') as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []
    logs.append(log_entry)
    _atomic_write_json(LOGGING_FILE, logs)


async def log_errors(error_type, error_message, function_name):
    """Log an error to the session log"""
    error_entry = {
        'timestamp': datetime.now(pytz.timezone('US/Central')).isoformat(),
        'type': error_type,
        'message': str(error_message),
        'function': function_name
    }
    session_log['errors'].append(error_entry)


def get_last_following_run():
    """Get the timestamp of the most recent successful following collection"""
    try:
        with open(LOGGING_FILE, 'r') as f:
            logs = json.load(f)

        for log_entry in reversed(logs):
            if (log_entry.get('status') == 'following_complete' and
                log_entry.get('following_collected', 0) > 0):
                timestamp_str = log_entry.get('timestamp')
                if timestamp_str:
                    # Robust parse for potential timezone-naive timestamps
                    try:
                        ts = datetime.fromisoformat(timestamp_str)
                    except Exception:
                        return None
                    if ts.tzinfo is None:
                        ts = pytz.timezone('US/Central').localize(ts)
                    return ts

        return None
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None


def should_run_following():
    """Determine if following collection should run (2-3 times per week)
        - if no previous run found, run it
        - if more than 7 days (168 hours) since last run, run it
        - if 3-7 days (72-168 hours) since last run, increase probability of running
        - if less than 3 days (72 hours) since last run, don't run
    """
    last_run = get_last_following_run()
    if last_run is None:
        return True

    cst = pytz.timezone('US/Central')
    current_time = datetime.now(cst)
    if last_run.tzinfo is None:
        last_run = cst.localize(last_run)
    else:
        last_run = last_run.astimezone(cst)

    hours_since = (current_time - last_run).total_seconds() / 3600
    if hours_since > 168:
        return True

    if hours_since >= 72:
        probability = (hours_since - 72) / 96
        return random.random() < probability
    return False


def extract_media_info(tweet):
    """Extract comprehensive media information from a tweet"""
    media_info = []
    if hasattr(tweet, 'media') and tweet.media:
        for media in tweet.media:
            media_data = {
                'type': getattr(media, 'type', 'unknown'),
                'url': getattr(media, 'url', ''),
                'media_url': getattr(media, 'media_url_https', getattr(media, 'media_url', '')),
                'display_url': getattr(media, 'display_url', ''),
                'expanded_url': getattr(media, 'expanded_url', ''),
                'sizes': getattr(media, 'sizes', {}),
                'video_info': getattr(media, 'video_info', None) if hasattr(media, 'video_info') else None
            }
            media_info.append(media_data)
    return media_info


async def ensure_authenticated():
    """Ensure client is authenticated, using cookies or fresh login"""
    session_log['attempts'] += 1
    try:
        client.load_cookies(COOKIES_FILE)
        return
    except Exception as e:
        print(f"Cookie loading failed: {e}; starting a new session")
        await log_errors('cookie_load_failed', str(e), 'ensure_authenticated')

    error_map = {
        twikit_errors.AccountLocked: ('account_locked', "Account is locked - may need captcha solving"),
        twikit_errors.AccountSuspended: ('account_suspended', "Account is suspended"),
        twikit_errors.Unauthorized: ('unauthorized', "Invalid credentials"),
        twikit_errors.TooManyRequests: ('rate_limited', "Rate limited - wait before retrying"),
    }

    try:
        await client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD,
            cookies_file=COOKIES_FILE,
            totp_secret=TOTP_SECRET
        )
        print("Login successful")
    except tuple(error_map.keys()) as e:
        error_type, message = error_map[type(e)]
        print(f"Error: {message}")
        await log_errors(error_type, str(e), 'ensure_authenticated')
        raise
    except Exception as e:
        print(f"Error: Login failed: {e}")
        await log_errors('login_failed', str(e), 'ensure_authenticated')
        raise


async def get_my_user_id():
    """Gets the current user's ID, caching it to avoid repeated API calls."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            if 'user_id' in config:
                return config['user_id']
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    print("no user ID in cache, gettin' from API")
    me = await client.get_user_by_screen_name(USERNAME)
    session_log['calls'] += 1
    if me:
        _atomic_write_json(CONFIG_FILE, {'user_id': me.id})
        return me.id
    return None


@handle_errors(default_return=[])
async def get_my_following():
    await ensure_authenticated()
    local_calls = 0

    try:
        with open(FOLLOWING_FILE, 'r') as f:
            existing_data = json.load(f)
            existing_ids = {user['id'] for user in existing_data}
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []
        existing_ids = set()

    my_user_id = await get_my_user_id()
    if not my_user_id:
        print("no user ID found, aborting following check")
        return []

    following = await client.get_user_following(user_id=my_user_id, count=200)
    local_calls += 1
    session_log['calls'] += 1

    new_following = []
    pages_without_new_users = 0
    max_empty_pages = 2

    while True:
        page_new_count = 0
        for friend in following:
            if friend.id not in existing_ids:
                friend_info = {
                    'username': friend.screen_name,
                    'url': f'https://twitter.com/{friend.screen_name}',
                    'name': friend.name,
                    'description': getattr(friend, 'description', ''),
                    'id': friend.id
                }
                new_following.append(friend_info)
                existing_ids.add(friend.id)
                page_new_count += 1

        if not (hasattr(following, 'next_cursor') and following.next_cursor):
            break
        if page_new_count == 0:
            pages_without_new_users += 1
            if pages_without_new_users >= max_empty_pages:
                break
        else:
            pages_without_new_users = 0

        await asyncio.sleep(random.uniform(2, 8))
        if random.random() < 0.1:
            await asyncio.sleep(random.uniform(15, 30))

        try:
            following = await following.next()
            local_calls += 1
            session_log['calls'] += 1
        except twikit_errors.TooManyRequests as e:
            print("rate limited, sleep for 60 seconds")
            await log_errors('rate_limited', str(e), 'get_my_following')
            await asyncio.sleep(60)
            continue
        except twikit_errors.ServerError as e:
            print("server error, sleep for 30 seconds")
            await log_errors('server_error', str(e), 'get_my_following')
            await asyncio.sleep(30)
            continue

    all_data = existing_data + new_following
    _atomic_write_json(FOLLOWING_FILE, all_data)

    session_log['new_following_count'] = len(new_following)
    print(f"added {len(new_following)} new followings. total following: {len(all_data)}")
    print(f"total calls in get_my_following: {local_calls}")
    return all_data


@handle_errors(default_return=[])
async def get_my_feed():
    await ensure_authenticated()
    calls = 0
    cst = pytz.timezone('US/Central')
    current_date = datetime.now(cst).strftime('%Y-%m-%d')

    try:
        with open(TWEETS_FILE, 'r') as f:
            existing_data = json.load(f)
            if current_date in existing_data:
                existing_tweets = {tweet['id'] for tweet in existing_data[current_date]}
            else:
                existing_tweets = set()
                existing_data[current_date] = []
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {current_date: []}
        existing_tweets = set()

    all_tweets = []
    target_tweets = 200
    session_duration = 3600  #i.e. 1 hour
    start_time = time.time()

    print(f"tryna get {target_tweets} tweets over {session_duration/60} mins")

    timeline = await client.get_timeline(count=200)
    calls += 1
    session_log['calls'] += 1

    while len(all_tweets) < target_tweets and (time.time() - start_time) < session_duration:
        try:
            batch_tweets = []
            # batching tweets logic: expand to 200 tweets per page, then pull tweets
            for tweet in timeline:
                if tweet.id not in existing_tweets:
                    main_media = extract_media_info(tweet)
                    quote_media = []
                    if hasattr(tweet, 'quote') and tweet.quote:
                        quote_media = extract_media_info(tweet.quote)
                    tweet_info = {
                        'id': tweet.id,
                        'text': tweet.text,
                        'author': tweet.user.screen_name,
                        'author_name': tweet.user.name,
                        'created_at': tweet.created_at,
                        'retweet_count': tweet.retweet_count,
                        'favorite_count': tweet.favorite_count,
                        'view_count': getattr(tweet, 'view_count', 0),
                        'media': main_media,
                        'quote_tweet': {
                            'id': tweet.quote.id if hasattr(tweet, 'quote') and tweet.quote else None,
                            'text': tweet.quote.text if hasattr(tweet, 'quote') and tweet.quote else None,
                            'author': tweet.quote.user.screen_name if hasattr(tweet, 'quote') and tweet.quote and hasattr(tweet.quote, 'user') else None,
                            'media': quote_media
                        } if hasattr(tweet, 'quote') and tweet.quote else None,
                        'entities': getattr(tweet, 'entities', {}),
                        'urls': getattr(tweet, 'urls', []),
                        'hashtags': getattr(tweet, 'hashtags', []),
                        'is_retweet': hasattr(tweet, 'retweeted_tweet') and tweet.retweeted_tweet is not None,
                        'is_quote': hasattr(tweet, 'quote') and tweet.quote is not None,
                        'lang': getattr(tweet, 'lang', 'unknown'),
                    }
                    batch_tweets.append(tweet_info)
                    existing_tweets.add(tweet.id)

            all_tweets.extend(batch_tweets)
            session_log['tweets_collected'] = len(all_tweets)
            print(f"pulled {len(batch_tweets)} new tweets. total tweets: {len(all_tweets)}/{target_tweets}")
            existing_data[current_date].extend(batch_tweets)
            _atomic_write_json(TWEETS_FILE, existing_data)

            # check for next page
            if not (hasattr(timeline, 'next_cursor') and timeline.next_cursor):
                print("no more tweets available")
                break

            # wait and get next page
            if len(all_tweets) < target_tweets and (time.time() - start_time) < session_duration:
                wait_time = random.randint(5, 30)
                print(f"waiting {wait_time}secs before next batch...")
                await asyncio.sleep(wait_time)
                timeline = await timeline.next()
                calls += 1
                session_log['calls'] += 1

        except twikit_errors.TooManyRequests as e:
            print("rate limit error, sleep for 5 mins")
            await log_errors('rate_limited', str(e), 'get_my_feed')
            await asyncio.sleep(300)
        except twikit_errors.ServerError as e:
            print("server error, sleep for 30 secs")
            await log_errors('server_error', str(e), 'get_my_feed')
            await asyncio.sleep(30)
        except twikit_errors.BadRequest as e:
            print(f"bad request: {e}")
            await log_errors('bad_request', str(e), 'get_my_feed')
            break
        except twikit_errors.Forbidden as e:
            print("access forbidden to timeline")
            await log_errors('forbidden', str(e), 'get_my_feed')
            break
        except Exception as e:
            print(f"unexpected error: {e}")
            await log_errors('unexpected_error', str(e), 'get_my_feed')
            await asyncio.sleep(10)

    elapsed_time = time.time() - start_time
    session_log['tweets_collected'] = len(all_tweets)
    print(f"session completed, collected {len(all_tweets)} tweets in {elapsed_time/60:.1f} minutes")
    print(f"total calls in get_my_feed: {calls}")

    return all_tweets


@handle_errors(default_return=None)
async def main_runner():
    cst = pytz.timezone('US/Central')
    session_log['session_id'] = f"session_{int(time.time())}"
    session_log['start_time'] = datetime.now(cst).isoformat()
    print(f"starting session: {session_log['session_id']}")
    await log_session_data('started')

    if should_run_following():
        print("running following collection...")
        following_result = await get_my_following()
        await log_session_data('following_complete', {
            'following_collected': len(following_result) if following_result else 0
        })
    else:
        print("skipping following collection (ran recently)")
        following_result = []
        session_log['new_following_count'] = 0
        await log_session_data('following_skipped', {
            'reason': 'recent_run',
            'following_collected': 0
        })

    tweets_result = await get_my_feed()
    await log_session_data('tweets_complete', {
        'tweets_collected': len(tweets_result) if tweets_result else 0
    })

    await log_session_data('completed', {
        'final_following_count': len(following_result) if following_result else 0,
        'final_tweets_count': len(tweets_result) if tweets_result else 0,
        'success': True
    })
    print(f"session {session_log['session_id']} successful, added to logging.json")


if __name__ == "__main__":
    asyncio.run(main_runner())
