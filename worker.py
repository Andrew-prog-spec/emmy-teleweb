import os
import asyncio
import pandas as pd
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
import logging
from io import BytesIO
import psycopg2
from psycopg2.extras import Json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ['postgres'],
        user=os.environ['postgres'],
        password=os.environ['6.$R4a#NwDpcvp%'],
        host=os.environ['db.dhncdkzrqhwtuscjjdke.supabase.co'],
        port=os.environ['5432']
    )

def load_sending_state():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sending_state WHERE id = 1")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row:
        return {
            'is_sending': row[1],
            'should_stop': row[2],
            'current_message': row[3],
            'total_messages': row[4],
            'messages_sent_successfully': row[5],
            'messages_failed': row[6],
            'start_time': row[7],
            'estimated_time_remaining': row[8],
            'current_recipient': row[9],
            'send_mode': row[10],
            'last_message_sent': row[11],
            'sending_speed': row[12],
            'is_paused': row[13],
            'pause_countdown': row[14]
        }
    return {
        'is_sending': False,
        'should_stop': False,
        'current_message': 0,
        'total_messages': 0,
        'messages_sent_successfully': 0,
        'messages_failed': 0,
        'start_time': None,
        'estimated_time_remaining': 0,
        'current_recipient': '',
        'send_mode': '',
        'last_message_sent': '',
        'sending_speed': 0,
        'is_paused': False,
        'pause_countdown': 0
    }

def save_sending_state(sending_state):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO sending_state (
            id, is_sending, should_stop, current_message, total_messages,
            messages_sent_successfully, messages_failed, start_time, estimated_time_remaining,
            current_recipient, send_mode, last_message_sent, sending_speed, is_paused, pause_countdown
        ) VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            is_sending = EXCLUDED.is_sending,
            should_stop = EXCLUDED.should_stop,
            current_message = EXCLUDED.current_message,
            total_messages = EXCLUDED.total_messages,
            messages_sent_successfully = EXCLUDED.messages_sent_successfully,
            messages_failed = EXCLUDED.messages_failed,
            start_time = EXCLUDED.start_time,
            estimated_time_remaining = EXCLUDED.estimated_time_remaining,
            current_recipient = EXCLUDED.current_recipient,
            send_mode = EXCLUDED.send_mode,
            last_message_sent = EXCLUDED.last_message_sent,
            sending_speed = EXCLUDED.sending_speed,
            is_paused = EXCLUDED.is_paused,
            pause_countdown = EXCLUDED.pause_countdown
    """, (
        sending_state['is_sending'],
        sending_state['should_stop'],
        sending_state['current_message'],
        sending_state['total_messages'],
        sending_state['messages_sent_successfully'],
        sending_state['messages_failed'],
        sending_state['start_time'],
        sending_state['estimated_time_remaining'],
        sending_state['current_recipient'],
        sending_state['send_mode'],
        sending_state['last_message_sent'],
        sending_state['sending_speed'],
        sending_state['is_paused'],
        sending_state['pause_countdown']
    ))
    conn.commit()
    cursor.close()
    conn.close()

async def pause_with_countdown(duration_seconds=120):
    sending_state = load_sending_state()
    sending_state['is_paused'] = True
    sending_state['pause_countdown'] = duration_seconds
    save_sending_state(sending_state)
    update_interval = 5
    while sending_state['pause_countdown'] > 0 and not sending_state['should_stop']:
        sleep_time = min(update_interval, sending_state['pause_countdown'])
        await asyncio.sleep(sleep_time)
        sending_state['pause_countdown'] -= sleep_time
        save_sending_state(sending_state)
    sending_state['is_paused'] = False
    sending_state['pause_countdown'] = 0
    save_sending_state(sending_state)

async def send_column_data(client, entity, data_series, col_name, delay=0):
    sending_state = load_sending_state()
    for i, value in enumerate(data_series, 1):
        if sending_state['should_stop']:
            logger.info("Stop signal received, halting column sending.")
            return
        sending_state['current_message'] = i
        sending_state['total_messages'] = len(data_series)
        if pd.isna(value):
            continue
        value_str = str(int(float(value))) if isinstance(value, (float, int)) and float(value) == int(float(value)) else str(value).strip()
        if value_str:
            sending_state['current_number'] = value_str
            try:
                await client.send_message(entity, value_str)
                sending_state['messages_sent_successfully'] += 1
                sending_state['last_message_sent'] = value_str
                if sending_state['messages_sent_successfully'] % 100 == 0:
                    await pause_with_countdown(120)
                if delay > 0:
                    await asyncio.sleep(delay)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
                try:
                    await client.send_message(entity, value_str)
                    sending_state['messages_sent_successfully'] += 1
                    sending_state['last_message_sent'] = value_str
                    if sending_state['messages_sent_successfully'] % 100 == 0:
                        await pause_with_countdown(120)
                except Exception as retry_e:
                    sending_state['messages_failed'] += 1
                    logger.error(f"Failed to send item {i} '{value_str}' after rate limit wait: {retry_e}")
            except Exception as e:
                sending_state['messages_failed'] += 1
                logger.error(f"Failed to send item {i} '{value_str}': {e}")
        save_sending_state(sending_state)

async def send_row_data(client, entity, df, delay=0):
    sending_state = load_sending_state()
    total_rows = len(df)
    for i, (index, row) in enumerate(df.iterrows(), 1):
        if sending_state['should_stop']:
            logger.info("Stop signal received, halting row sending.")
            return
        sending_state['current_message'] = i
        sending_state['total_messages'] = total_rows
        row_parts = []
        for col_name, value in row.items():
            if pd.isna(value):
                continue
            value_str = str(int(float(value))) if isinstance(value, (float, int)) and float(value) == int(float(value)) else str(value).strip()
            if value_str:
                if len(df.columns) == 1:
                    row_parts.append(value_str)
                else:
                    row_parts.append(f"{col_name}: {value_str}")
        if row_parts:
            message = " | ".join(row_parts)
            sending_state['current_number'] = message
            try:
                await client.send_message(entity, message)
                sending_state['messages_sent_successfully'] += 1
                sending_state['last_message_sent'] = message
                if sending_state['messages_sent_successfully'] % 100 == 0:
                    await pause_with_countdown(120)
                if delay > 0:
                    await asyncio.sleep(delay)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
                try:
                    await client.send_message(entity, message)
                    sending_state['messages_sent_successfully'] += 1
                    sending_state['last_message_sent'] = message
                    if sending_state['messages_sent_successfully'] % 100 == 0:
                        await pause_with_countdown(120)
                except Exception as retry_e:
                    sending_state['messages_failed'] += 1
                    logger.error(f"Failed to send row {i} '{message}' after rate limit wait: {retry_e}")
            except Exception as e:
                sending_state['messages_failed'] += 1
                logger.error(f"Failed to send row {i} '{message}': {e}")
        save_sending_state(sending_state)

async def send_messages(file_payload, manual_data, recipient, send_mode, session_string):
    API_ID = int(os.environ.get('TELEGRAM_API_ID', 0))
    API_HASH = os.environ.get('TELEGRAM_API_HASH', '')
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    sending_state = load_sending_state()
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("Not authorized in worker session")
            return
        entity = await client.get_entity(recipient)
        if file_payload:
            file_ext = file_payload['filename'].lower().rsplit('.', 1)[-1]
            file_data = BytesIO(file_payload['data'])
            if file_ext == 'xlsx':
                df = pd.read_excel(file_data, engine='openpyxl')
            elif file_ext == 'csv':
                df = pd.read_csv(file_data)
            elif file_ext == 'txt':
                text = file_data.getvalue().decode('utf-8').strip()
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                df = pd.DataFrame(lines, columns=['data'])
            else:
                logger.error("Unsupported file type")
                return
            if send_mode == 'rows':
                await send_row_data(client, entity, df)
            else:
                for column in df.columns:
                    if sending_state['should_stop']:
                        break
                    await send_column_data(client, entity, df[column], column)
        elif manual_data:
            lines = [line.strip() for line in manual_data.splitlines() if line.strip()]
            df = pd.DataFrame(lines, columns=['data'])
            await send_column_data(client, entity, df['data'], 'data')
    finally:
        sending_state['is_sending'] = False
        sending_state['should_stop'] = False
        save_sending_state(sending_state)
        await client.disconnect()

def extract_number_pattern(number_str):
    digits = ''.join(filter(str.isdigit, str(number_str)))
    return digits[-4:] if len(digits) >= 4 else digits.zfill(4)

def extract_otp_from_message(message_text):
    if not message_text:
        return None
    pattern1 = r'(\d{3})\s*-\s*(\d{3})'
    match = re.search(pattern1, message_text)
    if match:
        return match.group(1) + match.group(2)
    pattern2 = r'\b(\d{6})\b'
    match = re.search(pattern2, message_text)
    if match:
        return match.group(1)
    return None

async def search_groups_for_numbers(client, target_pattern=None, limit_groups=20, messages_per_group=200, after_timestamp=None):
    reply_state = load_reply_state()
    current_time = time.time()
    cache_ttl = 7200
    numbers_to_remove = []
    for number, refs in reply_state['group_numbers'].items():
        fresh_refs = [ref for ref in refs if (current_time - ref.get('cached_at', 0)) < cache_ttl]
        if fresh_refs:
            reply_state['group_numbers'][number] = fresh_refs
        else:
            numbers_to_remove.append(number)
    for number in numbers_to_remove:
        del reply_state['group_numbers'][number]
    save_reply_state(reply_state)
    
    total_numbers = 0
    groups_searched = 0
    dialogs = []
    if reply_state['target_groups']:
        async for dialog in client.iter_dialogs():
            if (dialog.is_group or dialog.is_channel) and dialog.entity.id in reply_state['target_groups']:
                dialogs.append(dialog)
    else:
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                dialogs.append(dialog)
    
    from datetime import datetime, timezone
    min_datetime = datetime.min.replace(tzinfo=timezone.utc)
    dialogs.sort(key=lambda d: d.date if d.date else min_datetime, reverse=True)
    groups_to_search = dialogs if reply_state['target_groups'] else dialogs[:limit_groups]
    
    for dialog in groups_to_search:
        peer_id = dialog.entity.id
        access_hash = getattr(dialog.entity, 'access_hash', None)
        messages_found = 0
        async for message in client.iter_messages(dialog, limit=messages_per_group):
            if message.text and messages_found < messages_per_group:
                if after_timestamp and after_timestamp > 1_000_000_000:
                    msg_timestamp = message.date.timestamp() if message.date else 0
                    if msg_timestamp < (after_timestamp - 300) or msg_timestamp > (after_timestamp + 30):
                        continue
                potential_numbers = re.findall(r'[\d\s\-\(\)\+\.]{4,}', message.text)
                numbers = [re.sub(r'\D', '', candidate) for candidate in potential_numbers if len(re.sub(r'\D', '', candidate)) >= 6]
                for number in numbers:
                    pattern = extract_number_pattern(number)
                    if target_pattern and pattern != target_pattern:
                        continue
                    message_ref = {
                        'peer_id': peer_id,
                        'access_hash': access_hash,
                        'msg_id': message.id,
                        'pattern': pattern,
                        'group_name': dialog.name,
                        'number': number,
                        'timestamp': message.date.timestamp() if message.date else 0,
                        'cached_at': time.time()
                    }
                    if pattern not in reply_state['group_numbers']:
                        reply_state['group_numbers'][pattern] = []
                    if len(reply_state['group_numbers'][pattern]) < 3:
                        reply_state['group_numbers'][pattern].append(message_ref)
                        total_numbers += 1
                    else:
                        oldest_ref = min(reply_state['group_numbers'][pattern], key=lambda r: r.get('timestamp', 0))
                        if message_ref['timestamp'] > oldest_ref.get('timestamp', 0):
                            reply_state['group_numbers'][pattern].remove(oldest_ref)
                            reply_state['group_numbers'][pattern].append(message_ref)
                    messages_found += 1
                    if target_pattern and pattern == target_pattern:
                        break
        groups_searched += 1
        if target_pattern and any(ref.get('pattern') == target_pattern for refs in reply_state['group_numbers'].values() for ref in refs):
            break
    save_reply_state(reply_state)
    return True

async def find_best_matching_message(target_pattern, original_number, after_timestamp=None):
    reply_state = load_reply_state()
    pattern_matches = []
    if target_pattern in reply_state['group_numbers']:
        for ref in reply_state['group_numbers'][target_pattern]:
            if after_timestamp:
                msg_timestamp = ref.get('timestamp', 0)
                if msg_timestamp < (after_timestamp - 300) or msg_timestamp > (after_timestamp + 30):
                    continue
            pattern_matches.append({
                'number': ref.get('number', original_number),
                'peer_id': ref['peer_id'],
                'access_hash': ref['access_hash'],
                'msg_id': ref['msg_id'],
                'group_name': ref['group_name'],
                'confidence': 'pattern'
            })
    if pattern_matches:
        return pattern_matches[0]
    return None