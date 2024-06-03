import asyncio
import os
import logging
import yaml
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors.rpcerrorlist import PhoneNumberInvalidError, ChannelInvalidError
import re
from pymongo import MongoClient
from datetime import datetime
import pytz
from pymongo.errors import ServerSelectionTimeoutError
from time import sleep
from telethon.tl.types import InputPeerChannel

# Load environment variables from .env file
load_dotenv()

# Check and create required files in the root directory
required_files = ['message_log.txt', 'config.yaml', 'last_message_id.txt']
for file in required_files:
    if not os.path.exists(file):
        with open(file, 'w') as f:
            logging.info(f"Created {file}")

# Configure logging with timestamps
logging.basicConfig(filename='message_handler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from config.yaml
with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Get Telegram client configuration from config
telegram_config = config.get('telegram', {})

# Extract configuration parameters
session_file_path = telegram_config.get('session_file_path')
alert_group_id = os.getenv('ALERT_GROUP_ID')  # Bot alert group ID
bot_token = os.getenv('BOT_TOKEN')  # Telegram bot token
bot_entity = InputPeerChannel(alert_group_id, None)  # Entity for the alert group

# Get API credentials and channel IDs from environment variables
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
source_channel_ids = [int(os.getenv('SOURCE_CHANNEL_ID_1')), int(os.getenv('SOURCE_CHANNEL_ID_2'))]

# Get MongoDB configuration from environment variables
mongo_uri = os.getenv('MONGO_URI')
mongo_db_name = os.getenv('MONGO_DB_NAME')
mongo_collection_name = os.getenv('MONGO_COLLECTION_NAME')

# Initialize the Telegram client
client = TelegramClient(session_file_path, api_id, api_hash)

# Initialize MongoDB client
mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=1000)
db = mongo_client[mongo_db_name]
collection = db[mongo_collection_name]

# Initialize an asynchronous queue
queue = asyncio.Queue()

# Define a function to send alert messages to the group
async def send_alert_message(message):
    await client.send_message(bot_entity, f"[Find&Store Snipe Data] {message}")

# Define a function to clean the message text
def clean_message(text):
    # Remove deep links
    text = ' '.join(word for word in text.split() if not word.startswith('https://t.me/'))
    # Remove regular links
    text = ' '.join(word for word in text.split() if not word.startswith('http'))
    return text

# Define a function to extract data from cleaned message text
def extract_data(message):
    data = {}
    
    try:
        # Extract Token Name and CA
        token_name_ca = re.search(r'🔮 Etherscan Tracker \| (.+) 📜 CA: (0x[0-9a-fA-F]+)', message)
        if token_name_ca:
            data['token_name'] = token_name_ca.group(1)
            data['contract_address'] = token_name_ca.group(2)
        # Extract Massive Buys Detected
        massive_buys = re.search(r'🏵 Massive Buys Detected: (\d+) txs in (\d+) secs', message)
        if massive_buys:
            data['massive_buys_detected'] = {
                'total_transactions': int(massive_buys.group(1)),
                'time_seconds': int(massive_buys.group(2))
            }
        
        # Extract Buys from different sources
        buys = re.findall(r'\|---(.+): (\d+) \| (.+) ETH \$(.+)', message)
        if buys:
            data['buys'] = []
            for buy in buys:
                try:
                    usd_value_str = re.search(r'\d+(\.\d+)?', buy[3].replace(',', ''))
                    if usd_value_str:
                        usd_value = float(usd_value_str.group())
                    else:
                        usd_value = 0.0
                    data['buys'].append({
                        'source': buy[0].strip(),
                        'transactions': int(buy[1]),
                        'eth': float(buy[2]),
                        'usd_value': usd_value
                    })
                except ValueError as e:
                    logging.error(f"Error parsing buy data: {buy}. Error: {e}")
        
        # Extract Wallet Analysis
        wallet_analysis = re.search(r'👤 Wallet Analysis\n- Fresh: (\d+) \| 15d: (\d+) \| 30d\+: (\d+) \| 60d\+: (\d+) \| 90d\+: (\d+) \| 180d\+: (\d+)', message)
        if wallet_analysis:
            data['wallet_analysis'] = {
                'fresh': int(wallet_analysis.group(1)),
                '15d': int(wallet_analysis.group(2)),
                '30d+': int(wallet_analysis.group(3)),
                '60d+': int(wallet_analysis.group(4)),
                '90d+': int(wallet_analysis.group(5)),
                '180d+': int(wallet_analysis.group(6))
            }
        
        eth_analysis = re.search(r'- Less than 1 ETH: (\d+) \| 1-10 ETH: (\d+) \| 10-50 ETH: (\d+) \| 50-100 ETH: (\d+) \| 100\+ ETH: (\d+)', message)
        if eth_analysis:
            data['eth_analysis'] = {
                'less_than_1_eth': int(eth_analysis.group(1)),
                '1_10_eth': int(eth_analysis.group(2)),
                '10_50_eth': int(eth_analysis.group(3)),
                '50_100_eth': int(eth_analysis.group(4)),
                '100_plus_eth': int(eth_analysis.group(5))
            }
        
    except Exception as e:
        logging.error(f"Error extracting data from message: {message}. Error: {e}")
    
    return data

# Define a function to get the last processed message ID
def get_last_processed_message_id():
    try:
        with open('last_message_id.txt', 'r') as file:
            return int(file.read().strip())
    except Exception as e:
        logging.error(f"Error reading last message ID from file: {e}")
        return None

# Define a function to set the last processed message ID
def set_last_processed_message_id(message_id):
    try:
        with open('last_message_id.txt', 'w') as file:
            file.write(str(message_id))
    except Exception as e:
        logging.error(f"Error writing last message ID to file: {e}")

# Define event handler for new messages
@client.on(events.NewMessage)
async def handler(event):
    chat_id = event.chat_id

    if chat_id in source_channel_ids:
        await queue.put(event)

# Define a function to
# Define a function to process messages from the queue
async def process_queue():
    while True:
        event = await queue.get()
        message_id = event.id

        # Convert timestamp to EAT timezone
        eat_tz = pytz.timezone('Africa/Nairobi')
        timestamp = datetime.now(tz=eat_tz).strftime('%Y-%m-%d %H:%M:%S %Z%z')

        cleaned_text = clean_message(event.raw_text)
        data = extract_data(cleaned_text)

        try:
            # Store message ID and timestamp in a text file
            with open('message_log.txt', 'a') as log_file:
                log_file.write(f'Message ID: {message_id}, Timestamp: {timestamp}\n')
            
            # Store extracted data in MongoDB with retry mechanism
            retry_count = 10  # Increase the retry count to 10
            while retry_count > 0:
                try:
                    collection.insert_one({
                        'message_id': message_id,
                        'timestamp': timestamp,
                        'data': data
                    })
                    logging.info(f"Message ID: {message_id}, Timestamp: {timestamp} - Data stored in MongoDB.")
                    set_last_processed_message_id(message_id)
                    break
                except ServerSelectionTimeoutError:
                    logging.error("MongoDB server selection timed out. Retrying...")
                    retry_count -= 1
                    sleep(1)
                except Exception as e:
                    logging.error(f"Error storing data in MongoDB for Message ID: {message_id}. Error: {e}")
                    await send_alert_message(f"Error storing data in MongoDB for Message ID: {message_id}. Error: {e}")
                    break  # Exit retry loop for other exceptions
            
            if retry_count == 0:
                logging.error(f"Failed to store data in MongoDB after retries for Message ID: {message_id}")
                await send_alert_message(f"Failed to store data in MongoDB after retries for Message ID: {message_id}")

        except Exception as e:
            logging.error(f"Error storing data in MongoDB for Message ID: {message_id}. Error: {e}")
            await send_alert_message(f"Error storing data in MongoDB for Message ID: {message_id}. Error: {e}")

        queue.task_done()

# Define the main function to run the client and queue processor
async def main():
    try:
        # Start the client
        await client.start()
        logging.info('Client started successfully.')

        # Fetch and process missed messages since last processed message ID
        last_message_id = get_last_processed_message_id()
        for channel_id in source_channel_ids:
            async for message in client.iter_messages(channel_id, min_id=last_message_id):
                await handler(message)

        # Create and start the queue processing task
        queue_task = asyncio.create_task(process_queue())

        # Run the client until disconnected
        await client.run_until_disconnected()
    except PhoneNumberInvalidError:
        logging.error('Invalid phone number provided.')
    except ChannelInvalidError:
        logging.error('Invalid channel ID provided.')
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        # Send an alert message for any other unhandled exceptions
        await send_alert_message(f'An error occurred: {e}')
    finally:
        # Disconnect the client before exiting
        await client.disconnect()
        logging.info('Client disconnected.')

# Run the main function
asyncio.run(main())
