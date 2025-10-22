import os
import asyncio
import pandas as pd
import sqlite3
import shutil
from flask import Flask, request, render_template_string, flash, redirect, url_for, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError, PhoneNumberInvalidError
from telethon.tl.types import InputPeerUser, InputPeerChannel
import secrets
import psycopg2
from psycopg2.extras import Json
from rq import Queue
from redis import Redis
import time
import re
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', secrets.token_hex(16))

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )

# Initialize database tables
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auth_state (
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(20),
            code_requested BOOLEAN,
            is_authenticated BOOLEAN,
            phone_code_hash TEXT,
            session_string TEXT,
            monitoring_session_string TEXT
        );
        INSERT INTO auth_state (id, phone_number, code_requested, is_authenticated, phone_code_hash, session_string, monitoring_session_string)
        VALUES (1, NULL, FALSE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS sending_state (
            id SERIAL PRIMARY KEY,
            is_sending BOOLEAN,
            should_stop BOOLEAN,
            current_message INTEGER,
            total_messages INTEGER,
            messages_sent_successfully INTEGER,
            messages_failed INTEGER,
            start_time FLOAT,
            estimated_time_remaining INTEGER,
            current_recipient TEXT,
            send_mode TEXT,
            last_message_sent TEXT,
            sending_speed FLOAT,
            is_paused BOOLEAN,
            pause_countdown INTEGER
        );
        INSERT INTO sending_state (id, is_sending, should_stop, current_message, total_messages, messages_sent_successfully, messages_failed, start_time, estimated_time_remaining, current_recipient, send_mode, last_message_sent, sending_speed, is_paused, pause_countdown)
        VALUES (1, FALSE, FALSE, 0, 0, 0, 0, NULL, 0, '', '', '', 0, FALSE, 0)
        ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS reply_state (
            id SERIAL PRIMARY KEY,
            monitoring BOOLEAN,
            target_recipient TEXT,
            target_groups JSONB,
            found_matches JSONB,
            group_numbers JSONB,
            processed_messages JSONB,
            replies_received JSONB,
            duplicate_replies JSONB,
            sending_start_times JSONB,
            duplicate_time_window INTEGER,
            number_timestamps JSONB,
            last_auto_reply JSONB,
            group_numbers_ttl JSONB
        );
        INSERT INTO reply_state (id, monitoring, target_recipient, target_groups, found_matches, group_numbers, processed_messages, replies_received, duplicate_replies, sending_start_times, duplicate_time_window, number_timestamps, last_auto_reply, group_numbers_ttl)
        VALUES (1, FALSE, NULL, '{}', '{}', '{}', '{}', '{}', '{}', '{}', 1800, '{}', '{}', '{}')
        ON CONFLICT (id) DO NOTHING;
    """)
    conn.commit()
    cursor.close()
    conn.close()

init_db()

# Redis connection for RQ
redis_conn = Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', 6379)),
    password=os.environ.get('REDIS_PASSWORD', None)
)
queue = Queue(connection=redis_conn)

# Telegram API credentials
API_ID = int(os.environ.get('25509235', 0)) if os.environ.get('TELEGRAM_API_ID') else None
API_HASH = os.environ.get('d3629ab967e8ecac197831192aa36d65', '')

# Shared CSS styles
SHARED_STYLES = """
    body {
        margin: 0;
        padding: 20px;
        font-family: Arial, sans-serif;
        background: linear-gradient(135deg, #0c0c3b, #1a1a5e, #0f0f4f);
        background-size: 400% 400%;
        animation: animeWave 15s ease infinite;
        color: white;
        min-height: 100vh;
    }
    @keyframes animeWave {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    .container { max-width: 600px; margin: 0 auto; background: rgba(0,0,0,0.5); padding: 20px; border-radius: 10px; }
    input, textarea, button { width: 100%; padding: 10px; margin: 10px 0; border: none; border-radius: 5px; }
    textarea { height: 100px; }
    button { background: #4a90e2; color: white; cursor: pointer; }
    button:hover { background: #357abd; }
    .error { color: #ff6b6b; }
    .success { color: #51cf66; }
    .nav-link { display: inline-block; padding: 10px 15px; margin: 5px; background: #2c2c54; border-radius: 5px; text-decoration: none; color: white; }
    .nav-link:hover { background: #40407a; }
    .stop-button { background: #e74c3c !important; margin-top: 10px; }
    .stop-button:hover { background: #c0392b !important; }
    .stop-button:disabled { background: #666 !important; cursor: not-allowed; }
    .sending-status { padding: 15px; margin: 15px 0; border-radius: 8px; background: rgba(255,255,255,0.1); display: none; border-left: 4px solid #4a90e2; }
    .progress-info { color: #51cf66; font-weight: bold; margin: 8px 0; }
    .progress-bar { width: 100%; height: 20px; background: rgba(255,255,255,0.2); border-radius: 10px; margin: 10px 0; overflow: hidden; }
    .progress-fill { height: 100%; background: linear-gradient(90deg, #4a90e2, #51cf66); transition: width 0.3s ease; }
    .status-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 15px 0; }
    .status-item { background: rgba(255,255,255,0.1); padding: 10px; border-radius: 5px; text-align: center; }
    .status-value { font-size: 18px; font-weight: bold; color: #51cf66; }
    .pulse { animation: pulse 2s infinite; }
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
    .error-count { color: #ff6b6b !important; }
    .success-count { color: #51cf66 !important; }
"""

# Authentication page template (simplified, move to templates/auth.html if preferred)
AUTH_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Telegram Authentication</title>
    <style>""" + SHARED_STYLES + """</style>
</head>
<body>
    <div class="container">
        <h1>Telegram File Sender</h1>
        <p>Please authenticate with Telegram to continue</p>
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <p class="{{ category }}">{{ message }}</p>
                {% endfor %}
            {% endif %}
        {% endwith %}
        {% if not code_requested %}
            <form method="POST" action="/request_code">
                <h2>Connect to Telegram</h2>
                <input type="text" name="phone" placeholder="Enter phone number with country code (e.g., +1234567890)" required>
                <button type="submit">Request Login Code</button>
            </form>
        {% else %}
            <form method="POST" action="/login">
                <h2>Enter Telegram Code</h2>
                <p>Code sent to: {{ phone_number }}</p>
                <input type="text" name="code" placeholder="Enter code from Telegram app" required>
                <button type="submit">Login</button>
            </form>
        {% endif %}
    </div>
</body>
</html>
"""

# Dashboard template (simplified, move to templates/dashboard.html if preferred)
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Telegram File Sender - Dashboard</title>
    <style>""" + SHARED_STYLES + """</style>
</head>
<body>
    <div class="container">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
            <h1>File Sender Dashboard</h1>
            <a href="/logout" class="nav-link">Logout</a>
        </div>
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <p class="{{ category }}">{{ message }}</p>
                {% endfor %}
            {% endif %}
        {% endwith %}
        <form method="POST" enctype="multipart/form-data" action="/upload">
            <h2>Upload File or Enter Data</h2>
            <label>XLSX, CSV or TXT File:</label>
            <input type="file" name="file" accept=".xlsx,.csv,.txt">
            <label>Or Enter Manual Data:</label>
            <textarea name="manual_data" placeholder="Enter data line by line (one per line)"></textarea>
            <label>Recipient:</label>
            <input type="text" name="recipient" placeholder="@username or @botname" required>
            <label>Send Mode:</label>
            <div style="margin: 10px 0;">
                <input type="radio" id="columns" name="send_mode" value="columns" checked>
                <label for="columns" style="display: inline; margin-left: 5px; margin-right: 20px;">Column by Column</label>
                <input type="radio" id="rows" name="send_mode" value="rows">
                <label for="rows" style="display: inline; margin-left: 5px;">Row by Row</label>
            </div>
            <button type="submit" id="sendButton">Send to Recipient</button>
            <div id="sendingStatus" class="sending-status">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h3 style="margin: 0; color: #4a90e2;" id="statusText">📤 Sending Messages...</h3>
                    <button type="button" id="stopButton" class="stop-button pulse">🛑 Stop Sending</button>
                </div>
                <div style="margin: 15px 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;">
                        <span id="progressInfo">Progress: 0/0</span>
                        <span id="progressPercent">0%</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" id="progressFill" style="width: 0%;"></div>
                    </div>
                </div>
                <div style="background: linear-gradient(135deg, #27ae60, #2ecc71); border-radius: 12px; padding: 20px; margin: 15px 0; text-align: center; box-shadow: 0 4px 15px rgba(39, 174, 96, 0.3);">
                    <div style="color: white; font-size: 14px; margin-bottom: 5px; font-weight: 500;">📨 TOTAL MESSAGES SENT</div>
                    <div style="color: white; font-size: 36px; font-weight: bold;" id="totalMessagesSent">0</div>
                    <div style="color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 5px;">This Session</div>
                </div>
                <div class="status-grid">
                    <div class="status-item">
                        <div class="status-value success-count" id="successCount">0</div>
                        <div>✅ Sent Successfully</div>
                    </div>
                    <div class="status-item">
                        <div class="status-value error-count" id="failedCount">0</div>
                        <div>❌ Failed</div>
                    </div>
                    <div class="status-item">
                        <div class="status-value" id="sendingSpeed">0</div>
                        <div>📊 Messages/min</div>
                    </div>
                    <div class="status-item">
                        <div class="status-value" id="timeRemaining">--:--</div>
                        <div>⏱️ Est. Remaining</div>
                    </div>
                </div>
                <div class="progress-info">
                    <strong>📱 Recipient:</strong> <span id="currentRecipient">--</span><br>
                    <strong>🔄 Mode:</strong> <span id="sendMode">--</span><br>
                    <strong>💬 Current:</strong> <span id="currentNumber">--</span><br>
                    <strong>📨 Last Sent:</strong> <span id="lastMessageSent" style="font-family: monospace; font-size: 12px;">--</span>
                </div>
            </div>
        </form>
        <div style="margin-top: 30px; padding-top: 20px; border-top: 2px solid rgba(255,255,255,0.2);">
            <h2>Reply Monitoring (Always Active)</h2>
            <p style="font-size: 14px; color: #ccc; margin-bottom: 15px;">
                ✅ <strong>Automatic monitoring is active!</strong> The system continuously monitors incoming replies and automatically responds when someone sends the same number twice.
            </p>
            <div style="margin: 20px 0; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 8px;">
                <h3 style="margin-top: 0; color: #4a90e2;">🎯 Set Target Recipient</h3>
                <div style="display: flex; gap: 10px; align-items: center;">
                    <input type="text" id="targetRecipientInput" placeholder="@username (e.g., @john_doe)" style="flex: 1; padding: 8px;">
                    <button type="button" id="setTargetButton" style="width: auto; padding: 8px 15px; background: #27ae60;">Set Target</button>
                </div>
                <div id="targetStatus" style="margin-top: 10px; font-size: 12px;"></div>
            </div>
            <div style="margin: 20px 0; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 8px;">
                <h3 style="margin-top: 0; color: #4a90e2;">📁 Set Target Groups</h3>
                <div style="display: flex; gap: 10px; align-items: center; margin-bottom: 10px;">
                    <button type="button" id="loadGroupsButton" style="width: auto; padding: 8px 15px; background: #3498db;">Load Groups</button>
                    <button type="button" id="clearGroupsButton" style="width: auto; padding: 8px 15px; background: #e74c3c;">Clear Selection</button>
                    <button type="button" id="saveTargetGroupsButton" style="width: auto; padding: 8px 15px; background: #27ae60;">Save Target Groups</button>
                </div>
                <div id="groupsContainer" style="max-height: 200px; overflow-y: auto; border: 1px solid rgba(255,255,255,0.2); border-radius: 5px; padding: 10px; margin: 10px 0; display: none;">
                    <div id="groupsList">
                        <p style="text-align: center; color: #ccc;">Click "Load Groups" to see available groups</p>
                    </div>
                </div>
                <div id="selectedGroupsDisplay" style="margin-top: 10px; padding: 10px; background: rgba(0,0,0,0.3); border-radius: 5px; min-height: 30px;">
                    <strong>Currently Selected:</strong> <span id="selectedGroupsText">No target groups selected</span>
                </div>
                <div id="groupsStatus" style="margin-top: 10px; font-size: 12px;"></div>
            </div>
            <div style="display: flex; align-items: center; gap: 15px; margin: 15px 0; padding: 10px; background: rgba(39, 174, 96, 0.2); border-radius: 8px; border-left: 4px solid #27ae60;">
                <span style="color: #27ae60; font-size: 18px;">🟢</span>
                <span style="color: #27ae60; font-weight: bold;">Always Monitoring</span>
            </div>
            <div id="monitoringStatus" class="sending-status" style="display: block;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin: 10px 0;">
                    <div class="progress-info">
                        <strong>Status:</strong> <span id="monitoringState">Not monitoring</span>
                    </div>
                    <div class="progress-info">
                        <strong>Total Replies:</strong> <span id="totalReplies">0</span>
                    </div>
                    <div class="progress-info">
                        <strong>Duplicates Found:</strong> <span id="duplicateCount">0</span>
                    </div>
                    <div class="progress-info">
                        <strong>Auto-Replies Sent:</strong> <span id="matchesFound">0</span>
                    </div>
                </div>
                <div class="progress-info">
                    <strong>Numbers in Groups:</strong> <span id="groupNumbersCount">0</span>
                </div>
            </div>
        </div>
    </div>
    <script>
        let sendingInterval;
        document.querySelector('form[action="/upload"]').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const recipient = formData.get('recipient');
            if (!recipient || recipient.trim() === '') {
                alert('Please enter a recipient.');
                return;
            }
            const file = formData.get('file');
            const manualData = formData.get('manual_data');
            if ((!file || file.name === '') && (!manualData || manualData.trim() === '')) {
                alert('Please provide either a file (XLSX, CSV, or TXT) or manual data.');
                return;
            }
            document.getElementById('sendButton').disabled = true;
            document.getElementById('sendButton').textContent = 'Starting...';
            document.getElementById('sendingStatus').style.display = 'block';
            document.getElementById('statusText').textContent = '🚀 Starting message sending...';
            fetch('/upload', {
                method: 'POST',
                headers: { 'X-Requested-With': 'XMLHttpRequest' },
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    document.getElementById('statusText').textContent = '📤 Sending Messages...';
                    if (!sendingInterval) {
                        sendingInterval = setInterval(checkSendingStatus, 500);
                    }
                } else {
                    throw new Error(data.message || 'Failed to start sending');
                }
            })
            .catch(error => {
                document.getElementById('statusText').textContent = '❌ Error: ' + error.message;
                document.getElementById('statusText').style.color = '#e74c3c';
                setTimeout(() => {
                    document.getElementById('statusText').style.color = '#4a90e2';
                    document.getElementById('statusText').textContent = '📤 Sending Messages...';
                }, 5000);
                document.getElementById('sendButton').disabled = false;
                document.getElementById('sendButton').textContent = 'Send to Recipient';
                document.getElementById('sendingStatus').style.display = 'none';
            });
        });
        document.getElementById('stopButton').addEventListener('click', function() {
            if (confirm('Are you sure you want to stop sending messages?')) {
                fetch('/stop', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'}
                })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        document.getElementById('statusTe