# -*- coding: utf-8 -*-
"""
Telegram Bridge Bot - A messaging bridge between users and admin.

This bot forwards user messages to an admin and allows the admin to reply
back to users through the bot. All interactions are in Persian (Farsi).

Author: Your Name
License: MIT
"""

import logging
import asyncio
import time
from typing import Optional, Dict, Tuple
from collections import OrderedDict
from functools import wraps
from decouple import config
import redis.asyncio as aioredis
from telethon import TelegramClient, events

# Try to use uvloop for better performance (Linux/macOS only)
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False
from telethon.tl.types import (
    User,
    PeerUser,
    Message,
)
from telethon.errors import (
    FloodWaitError,
    UserIsBlockedError,
    ChatWriteForbiddenError,
    MessageIdInvalidError,
)

# =============================================================================
# Configuration
# =============================================================================

# Bot credentials - Load from .env file using python-decouple
# Get these from https://my.telegram.org/apps
API_ID: int = config("API_ID", cast=int, default=0)
API_HASH: str = config("API_HASH", default="")
BOT_TOKEN: str = config("BOT_TOKEN", default="")

# Admin chat ID - Can be a private user ID, group ID, or channel ID
# For groups/channels, use the negative ID format (e.g., -1001234567890)
ADMIN_CHAT_ID: int = config("ADMIN_CHAT_ID", cast=int, default=0)

# Session name for the bot
SESSION_NAME: str = config("SESSION_NAME", default="bridge_bot")

# Redis configuration for blocking system
REDIS_HOST: str = config("REDIS_HOST", default="localhost")
REDIS_PORT: int = config("REDIS_PORT", cast=int, default=6379)
REDIS_DB: int = config("REDIS_DB", cast=int, default=0)
REDIS_PASSWORD: Optional[str] = config("REDIS_PASSWORD", default=None)
REDIS_MAX_CONNECTIONS: int = config("REDIS_MAX_CONNECTIONS", cast=int, default=20)
REDIS_SOCKET_TIMEOUT: int = config("REDIS_SOCKET_TIMEOUT", cast=int, default=5)
REDIS_SOCKET_CONNECT_TIMEOUT: int = config("REDIS_SOCKET_CONNECT_TIMEOUT", cast=int, default=5)

# Rate limiting configuration
RATE_LIMIT_ENABLED: bool = config("RATE_LIMIT_ENABLED", cast=bool, default=True)
RATE_LIMIT_MAX_MESSAGES: int = config("RATE_LIMIT_MAX_MESSAGES", cast=int, default=10)
RATE_LIMIT_WINDOW: int = config("RATE_LIMIT_WINDOW", cast=int, default=60)  # seconds

# =============================================================================
# Logging Configuration
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# Message Mapping Storage (LRU Cache with size limit)
# =============================================================================

# Maximum number of message mappings to keep in memory
MAX_MESSAGE_MAPPINGS: int = config("MAX_MESSAGE_MAPPINGS", cast=int, default=10000)


class LRUCache:
    """
    LRU Cache implementation for message mappings.
    
    Automatically removes oldest entries when size limit is reached.
    """
    
    def __init__(self, max_size: int = MAX_MESSAGE_MAPPINGS):
        """
        Initialize LRU Cache.
        
        Args:
            max_size: Maximum number of items to store.
        """
        self.cache: OrderedDict = OrderedDict()
        self.max_size = max_size
    
    def get(self, key: int) -> Optional[int]:
        """
        Get value from cache and move to end (most recently used).
        
        Args:
            key: The key to look up.
            
        Returns:
            The value if found, None otherwise.
        """
        if key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return self.cache[key]
        return None
    
    def set(self, key: int, value: int) -> None:
        """
        Set value in cache.
        
        Args:
            key: The key.
            value: The value to store.
        """
        if key in self.cache:
            # Update existing and move to end
            self.cache.move_to_end(key)
        else:
            # Add new entry
            if len(self.cache) >= self.max_size:
                # Remove oldest (first) item
                self.cache.popitem(last=False)
        self.cache[key] = value
    
    def delete(self, key: int) -> bool:
        """
        Delete key from cache.
        
        Args:
            key: The key to delete.
            
        Returns:
            True if key was deleted, False if not found.
        """
        if key in self.cache:
            del self.cache[key]
            return True
        return False
    
    def clear(self) -> None:
        """Clear all entries from cache."""
        self.cache.clear()
    
    def size(self) -> int:
        """Get current cache size."""
        return len(self.cache)


# Maps forwarded message ID in admin chat -> original user ID
# Format: {forwarded_msg_id: user_id}
message_mapping: LRUCache = LRUCache(max_size=MAX_MESSAGE_MAPPINGS)

# Maps forwarded message ID -> original message ID in user's chat
# Format: {forwarded_msg_id: original_msg_id}
original_message_mapping: LRUCache = LRUCache(max_size=MAX_MESSAGE_MAPPINGS)

# =============================================================================
# Persian Messages
# =============================================================================

MESSAGES = {
    "welcome": (
        "🌟 خوش آمدید!\n\n"
        "هر پیامی که بفرستید، به ادمین ارسال می‌شود و پاسخ او را دریافت خواهید کرد.\n\n"
        "📩 منتظر پیام شما هستیم..."
    ),
    "message_sent": "✅ پیام شما با موفقیت ارسال شد.",
    "message_failed": "❌ متأسفانه ارسال پیام با مشکل مواجه شد. لطفاً دوباره تلاش کنید.",
    "reply_sent": "✅ پاسخ شما با موفقیت ارسال شد.",
    "reply_failed": "❌ ارسال پاسخ با مشکل مواجه شد. کاربر ممکن است ربات را بلاک کرده باشد.",
    "user_not_found": "❌ کاربر اصلی پیدا نشد. لطفاً به یک پیام فوروارد شده پاسخ دهید.",
    "admin_message_ignored": "ℹ️ برای پاسخ به کاربر، روی پیام فوروارد شده ریپلای کنید.",
    "user_blocked": "🚫 شما بلاک شده‌اید و نمی‌توانید پیام ارسال کنید.",
    "user_blocked_success": "✅ کاربر با موفقیت بلاک شد.",
    "user_unblocked_success": "✅ کاربر با موفقیت آنبلاک شد.",
    "user_already_blocked": "⚠️ این کاربر قبلاً بلاک شده است.",
    "user_not_blocked": "⚠️ این کاربر بلاک نشده است.",
    "block_error": "❌ خطا در بلاک کردن کاربر. لطفاً به یک پیام فوروارد شده ریپلای کنید.",
}

# =============================================================================
# Redis Block Manager
# =============================================================================


class BlockManager:
    """
    Manages user blocking/unblocking using Redis.
    
    Uses Redis to store blocked user IDs with a simple key-value structure.
    Key format: "blocked:user:{user_id}"
    Value: "1" (blocked) or None (not blocked)
    """
    
    def __init__(self):
        """Initialize Redis connection."""
        self.redis: Optional[aioredis.Redis] = None
        self._connection_pool: Optional[aioredis.ConnectionPool] = None
        self._connected: bool = False
    
    async def connect(self) -> None:
        """
        Connect to Redis server.
        
        Raises:
            redis.ConnectionError: If connection to Redis fails.
        """
        try:
            self._connection_pool = aioredis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                max_connections=REDIS_MAX_CONNECTIONS,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                retry_on_timeout=True,
                health_check_interval=30,
            )
            self.redis = aioredis.Redis(connection_pool=self._connection_pool)
            # Test connection
            await self.redis.ping()
            self._connected = True
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._connected = False
            raise
    
    async def health_check(self) -> bool:
        """
        Check Redis connection health.
        
        Returns:
            True if Redis is healthy, False otherwise.
        """
        if not self.redis or not self._connected:
            return False
        try:
            await self.redis.ping()
            return True
        except Exception:
            self._connected = False
            return False
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        self._connected = False
        if self.redis:
            try:
                await self.redis.close()
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
        if self._connection_pool:
            try:
                await self._connection_pool.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting Redis pool: {e}")
        logger.info("Disconnected from Redis")
    
    def _get_key(self, user_id: int) -> str:
        """
        Get Redis key for a user ID.
        
        Args:
            user_id: The user ID.
            
        Returns:
            Redis key string.
        """
        return f"blocked:user:{user_id}"
    
    async def is_blocked(self, user_id: int) -> bool:
        """
        Check if a user is blocked.
        
        Args:
            user_id: The user ID to check.
            
        Returns:
            True if user is blocked, False otherwise.
        """
        if not self.redis or not self._connected:
            # If Redis is not available, check health and try to reconnect
            if not await self.health_check():
                return False
        
        try:
            key = self._get_key(user_id)
            result = await self.redis.get(key)
            return result == "1"
        except Exception as e:
            logger.error(f"Error checking if user {user_id} is blocked: {e}")
            self._connected = False
            return False
    
    async def block_user(self, user_id: int) -> bool:
        """
        Block a user.
        
        Args:
            user_id: The user ID to block.
            
        Returns:
            True if user was blocked successfully, False otherwise.
        """
        if not self.redis or not self._connected:
            if not await self.health_check():
                logger.error("Redis not connected")
                return False
        
        try:
            key = self._get_key(user_id)
            await self.redis.set(key, "1")
            logger.info(f"User {user_id} blocked successfully")
            return True
        except Exception as e:
            logger.error(f"Error blocking user {user_id}: {e}")
            self._connected = False
            return False
    
    async def unblock_user(self, user_id: int) -> bool:
        """
        Unblock a user.
        
        Args:
            user_id: The user ID to unblock.
            
        Returns:
            True if user was unblocked successfully, False otherwise.
        """
        if not self.redis or not self._connected:
            if not await self.health_check():
                logger.error("Redis not connected")
                return False
        
        try:
            key = self._get_key(user_id)
            result = await self.redis.delete(key)
            logger.info(f"User {user_id} unblocked successfully")
            return result > 0
        except Exception as e:
            logger.error(f"Error unblocking user {user_id}: {e}")
            self._connected = False
            return False


# Initialize block manager
block_manager = BlockManager()

# =============================================================================
# Rate Limiting
# =============================================================================


class RateLimiter:
    """
    Simple in-memory rate limiter to prevent spam.
    
    Tracks message counts per user within a time window.
    """
    
    def __init__(self, max_messages: int = RATE_LIMIT_MAX_MESSAGES, 
                 window: int = RATE_LIMIT_WINDOW):
        """
        Initialize rate limiter.
        
        Args:
            max_messages: Maximum number of messages allowed per window.
            window: Time window in seconds.
        """
        self.max_messages = max_messages
        self.window = window
        self.user_timestamps: Dict[int, list] = {}
        self._lock = asyncio.Lock()
    
    async def is_allowed(self, user_id: int) -> Tuple[bool, Optional[int]]:
        """
        Check if user is allowed to send a message.
        
        Args:
            user_id: The user ID to check.
            
        Returns:
            Tuple of (is_allowed, remaining_seconds).
            remaining_seconds is None if allowed, otherwise seconds until allowed.
        """
        if not RATE_LIMIT_ENABLED:
            return True, None
        
        async with self._lock:
            now = time.time()
            
            # Clean old timestamps
            if user_id in self.user_timestamps:
                self.user_timestamps[user_id] = [
                    ts for ts in self.user_timestamps[user_id]
                    if now - ts < self.window
                ]
            
            # Check if limit exceeded
            if user_id in self.user_timestamps:
                count = len(self.user_timestamps[user_id])
                if count >= self.max_messages:
                    # Calculate remaining time
                    oldest = min(self.user_timestamps[user_id])
                    remaining = int(self.window - (now - oldest)) + 1
                    return False, remaining
                # Add current timestamp
                self.user_timestamps[user_id].append(now)
            else:
                # First message from this user
                self.user_timestamps[user_id] = [now]
            
            return True, None
    
    async def reset(self, user_id: int) -> None:
        """
        Reset rate limit for a user.
        
        Args:
            user_id: The user ID to reset.
        """
        async with self._lock:
            if user_id in self.user_timestamps:
                del self.user_timestamps[user_id]


# Initialize rate limiter
rate_limiter = RateLimiter(
    max_messages=RATE_LIMIT_MAX_MESSAGES,
    window=RATE_LIMIT_WINDOW
)

# =============================================================================
# Retry Logic
# =============================================================================


async def retry_async(
    func,
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
) -> any:
    """
    Retry an async function with exponential backoff.
    
    Args:
        func: The async function to retry.
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier.
        exceptions: Tuple of exceptions to catch and retry.
        
    Returns:
        The result of the function call.
        
    Raises:
        The last exception if all retries fail.
    """
    last_exception = None
    current_delay = delay
    
    for attempt in range(max_retries):
        try:
            return await func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries - 1:
                logger.warning(
                    f"Retry attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {current_delay:.2f}s..."
                )
                await asyncio.sleep(current_delay)
                current_delay *= backoff
            else:
                logger.error(f"All {max_retries} retry attempts failed: {e}")
    
    raise last_exception

# =============================================================================
# Initialize Bot Client
# =============================================================================

bot = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# =============================================================================
# Helper Functions
# =============================================================================


def format_user_info(user: User) -> str:
    """
    Format user information for admin notification.

    Args:
        user: The Telegram User object.

    Returns:
        Formatted string with user details in Persian.

    Example:
        >>> user_info = format_user_info(user)
        >>> print(user_info)
        👤 نام کاربر: علی رضایی
        🆔 آیدی کاربر: 123456789
        📛 یوزرنیم: @ali_rezaei
    """
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    full_name = f"{first_name} {last_name}".strip() or "نامشخص"
    
    username_display = f"@{user.username}" if user.username else "ندارد"
    
    return (
        f"👤 نام کاربر: {full_name}\n"
        f"🆔 آیدی کاربر: {user.id}\n"
        f"📛 یوزرنیم: {username_display}"
    )


async def is_admin_chat(chat_id: int) -> bool:
    """
    Check if the given chat ID is the admin chat.

    Args:
        chat_id: The chat ID to check.

    Returns:
        True if it's the admin chat, False otherwise.
    """
    return chat_id == ADMIN_CHAT_ID


async def forward_to_admin(event: events.NewMessage.Event) -> Optional[Message]:
    """
    Forward a user's message to the admin chat with retry logic.

    Args:
        event: The incoming message event from the user.

    Returns:
        The forwarded message object if successful, None otherwise.

    Raises:
        FloodWaitError: If rate limited by Telegram.
        ChatWriteForbiddenError: If bot cannot write to admin chat.
    """
    async def _forward():
        return await event.message.forward_to(ADMIN_CHAT_ID)
    
    try:
        forwarded_msg = await retry_async(
            _forward,
            max_retries=3,
            delay=1.0,
            backoff=2.0,
            exceptions=(Exception,)
        )
        return forwarded_msg
    except FloodWaitError as e:
        logger.warning(f"Flood wait error: need to wait {e.seconds} seconds")
        raise
    except ChatWriteForbiddenError:
        logger.error(f"Cannot write to admin chat: {ADMIN_CHAT_ID}")
        raise
    except Exception as e:
        logger.error(f"Error forwarding message to admin after retries: {e}")
        return None


async def send_user_info_to_admin(
    forwarded_msg: Message, 
    user: User
) -> Optional[Message]:
    """
    Send user information as a reply to the forwarded message in admin chat with retry.

    Args:
        forwarded_msg: The forwarded message in admin chat.
        user: The original sender's User object.

    Returns:
        The sent info message if successful, None otherwise.
    """
    async def _send_info():
        user_info = format_user_info(user)
        return await bot.send_message(
            ADMIN_CHAT_ID,
            user_info,
            reply_to=forwarded_msg.id,
        )
    
    try:
        info_msg = await retry_async(
            _send_info,
            max_retries=2,
            delay=0.5,
            backoff=2.0,
            exceptions=(Exception,)
        )
        return info_msg
    except Exception as e:
        logger.error(f"Error sending user info to admin after retries: {e}")
        return None


async def forward_to_user(
    user_id: int, 
    message: Message
) -> Optional[Message]:
    """
    Forward or copy admin's reply message to the original user with retry.

    Args:
        user_id: The original user's ID.
        message: The admin's reply message.

    Returns:
        The sent message if successful, None otherwise.

    Raises:
        UserIsBlockedError: If the user has blocked the bot.
    """
    async def _send_to_user():
        # Send a copy of the message instead of forwarding to hide admin identity
        return await bot.send_message(
            user_id,
            message.message,
            file=message.media if message.media else None,
        )
    
    try:
        sent_msg = await retry_async(
            _send_to_user,
            max_retries=2,
            delay=0.5,
            backoff=2.0,
            exceptions=(Exception,)
        )
        return sent_msg
    except UserIsBlockedError:
        logger.warning(f"User {user_id} has blocked the bot")
        raise
    except Exception as e:
        logger.error(f"Error sending message to user {user_id} after retries: {e}")
        return None


# =============================================================================
# Event Handlers
# =============================================================================


@bot.on(events.NewMessage(pattern="/start"))
async def handle_start(event: events.NewMessage.Event) -> None:
    """
    Handle the /start command.

    Sends a welcome message in Persian to new users.

    Args:
        event: The incoming /start command event.
    """
    # Only respond to private messages
    if not event.is_private:
        return
    
    # Ignore if from admin chat
    if await is_admin_chat(event.chat_id):
        return
    
    sender = await event.get_sender()
    logger.info(f"New user started bot: {sender.id} ({sender.first_name})")
    
    await event.respond(MESSAGES["welcome"])
    raise events.StopPropagation


@bot.on(events.NewMessage(pattern=r"/block\s*$"))
async def handle_block(event: events.NewMessage.Event) -> None:
    """
    Handle the /block command.

    Blocks a user when admin replies to their forwarded message with /block.

    Args:
        event: The incoming /block command event.
    """
    # Only handle from admin chat
    if not await is_admin_chat(event.chat_id):
        return
    
    # Must be a reply to a forwarded message
    if not event.message.reply_to:
        await event.respond(MESSAGES["block_error"])
        return
    
    reply_to_msg_id = event.message.reply_to.reply_to_msg_id
    
    # Find the original user ID from the mapping
    original_user_id = message_mapping.get(reply_to_msg_id)
    
    if not original_user_id:
        # Try reply_to_msg_id - 1 (in case replied to info message)
        original_user_id = message_mapping.get(reply_to_msg_id - 1)
    
    if not original_user_id:
        await event.respond(MESSAGES["block_error"])
        return
    
    # Check if already blocked
    if await block_manager.is_blocked(original_user_id):
        await event.respond(MESSAGES["user_already_blocked"])
        return
    
    # Block the user
    success = await block_manager.block_user(original_user_id)
    
    if success:
        await event.respond(MESSAGES["user_blocked_success"])
        logger.info(f"User {original_user_id} blocked by admin")
    else:
        await event.respond(MESSAGES["block_error"])


@bot.on(events.NewMessage(pattern=r"/unblock\s*$"))
async def handle_unblock(event: events.NewMessage.Event) -> None:
    """
    Handle the /unblock command.

    Unblocks a user when admin replies to their forwarded message with /unblock.

    Args:
        event: The incoming /unblock command event.
    """
    # Only handle from admin chat
    if not await is_admin_chat(event.chat_id):
        return
    
    # Must be a reply to a forwarded message
    if not event.message.reply_to:
        await event.respond(MESSAGES["block_error"])
        return
    
    reply_to_msg_id = event.message.reply_to.reply_to_msg_id
    
    # Find the original user ID from the mapping
    original_user_id = message_mapping.get(reply_to_msg_id)
    
    if not original_user_id:
        # Try reply_to_msg_id - 1 (in case replied to info message)
        original_user_id = message_mapping.get(reply_to_msg_id - 1)
    
    if not original_user_id:
        await event.respond(MESSAGES["block_error"])
        return
    
    # Check if not blocked
    if not await block_manager.is_blocked(original_user_id):
        await event.respond(MESSAGES["user_not_blocked"])
        return
    
    # Unblock the user
    success = await block_manager.unblock_user(original_user_id)
    
    if success:
        await event.respond(MESSAGES["user_unblocked_success"])
        logger.info(f"User {original_user_id} unblocked by admin")
    else:
        await event.respond(MESSAGES["block_error"])


@bot.on(events.NewMessage(incoming=True))
async def handle_user_message(event: events.NewMessage.Event) -> None:
    """
    Handle incoming messages from users.

    Forwards the message to admin and sends confirmation to user.

    Args:
        event: The incoming message event.
    """
    # Check if this is from admin chat (group or private)
    # If yes, handle it as a potential reply from admin/group members
    if await is_admin_chat(event.chat_id):
        await handle_admin_message(event)
        return
    
    # Only handle private messages from regular users
    if not event.is_private:
        return
    
    # Ignore commands (already handled by /start)
    if event.message.message and event.message.message.startswith("/"):
        return
    
    sender = await event.get_sender()
    if not sender:
        logger.warning("Could not get sender information")
        return
    
    # Check rate limit
    if RATE_LIMIT_ENABLED:
        allowed, remaining = await rate_limiter.is_allowed(sender.id)
        if not allowed:
            logger.warning(
                f"Rate limit exceeded for user {sender.id}. "
                f"Must wait {remaining} seconds."
            )
            await event.respond(
                f"⏱️ شما بیش از حد مجاز پیام ارسال کرده‌اید. "
                f"لطفاً {remaining} ثانیه صبر کنید."
            )
            return
    
    # Check if user is blocked
    if await block_manager.is_blocked(sender.id):
        logger.info(f"Blocked user {sender.id} tried to send a message")
        await event.respond(MESSAGES["user_blocked"])
        return
    
    logger.info(f"Received message from user {sender.id}")
    
    try:
        # Forward the message to admin
        forwarded_msg = await forward_to_admin(event)
        
        if forwarded_msg:
            # Store the mapping for later reply tracking (using LRU cache)
            message_mapping.set(forwarded_msg.id, sender.id)
            original_message_mapping.set(forwarded_msg.id, event.message.id)
            
            # Send user info as reply to forwarded message
            await send_user_info_to_admin(forwarded_msg, sender)
            
            # Send confirmation to user
            await event.respond(MESSAGES["message_sent"])
            
            logger.info(
                f"Message from {sender.id} forwarded to admin "
                f"(forwarded_msg_id: {forwarded_msg.id})"
            )
        else:
            await event.respond(MESSAGES["message_failed"])
            
    except FloodWaitError as e:
        logger.error(f"Rate limited, waiting {e.seconds} seconds")
        await event.respond(MESSAGES["message_failed"])
    except ChatWriteForbiddenError:
        logger.error("Cannot write to admin chat")
        await event.respond(MESSAGES["message_failed"])
    except Exception as e:
        logger.error(f"Unexpected error handling user message: {e}")
        await event.respond(MESSAGES["message_failed"])


async def handle_admin_message(event: events.NewMessage.Event) -> None:
    """
    Handle messages from the admin chat (group or private).

    If any member of the admin chat replies to a forwarded message, 
    sends the reply to the original user. Otherwise, ignores the message.

    This allows any member of the support group to reply to user messages.

    Args:
        event: The incoming message event from admin chat.
    """
    # Check if this is a reply to another message
    if not event.message.reply_to:
        # Not a reply - ignore non-reply messages from admin chat
        logger.debug(
            f"Message from admin chat (chat_id: {event.chat_id}) "
            "is not a reply, ignoring"
        )
        return
    
    reply_to_msg_id = event.message.reply_to.reply_to_msg_id
    
    # Check if the replied message is in our mapping (using LRU cache)
    # First, check if it's a reply to the forwarded message itself
    original_user_id = message_mapping.get(reply_to_msg_id)
    
    if not original_user_id:
        # Maybe replied to the info message, try to find the forwarded msg
        # by checking if reply_to_msg_id - 1 exists (info is sent right after forward)
        original_user_id = message_mapping.get(reply_to_msg_id - 1)
    
    if not original_user_id:
        logger.warning(
            f"Could not find original user for reply_to_msg_id: {reply_to_msg_id} "
            f"in chat {event.chat_id}"
        )
        # Only send error message if it's a private chat (not in group to avoid spam)
        if event.is_private:
            await event.respond(MESSAGES["user_not_found"])
        return
    
    # Get sender information for logging
    sender = await event.get_sender()
    sender_name = f"{sender.first_name} {sender.last_name or ''}".strip() if sender else "Unknown"
    sender_id = sender.id if sender else "Unknown"
    
    logger.info(
        f"Reply from admin chat member {sender_id} ({sender_name}) "
        f"to user {original_user_id}"
    )
    
    try:
        # Send the reply to the original user
        await forward_to_user(original_user_id, event.message)
        
        # Confirm to sender that message was sent
        # Only send confirmation in private chats to avoid spam in groups
        if event.is_private:
            await event.respond(MESSAGES["reply_sent"])
        
        logger.info(
            f"Reply from {sender_id} sent to user {original_user_id} successfully"
        )
        
    except UserIsBlockedError:
        # User has blocked the bot - notify sender
        if event.is_private:
            await event.respond(MESSAGES["reply_failed"])
        logger.warning(f"User {original_user_id} has blocked the bot")
    except Exception as e:
        logger.error(f"Error sending reply to user {original_user_id}: {e}")
        if event.is_private:
            await event.respond(MESSAGES["reply_failed"])


# =============================================================================
# Main Entry Point
# =============================================================================


async def main() -> None:
    """
    Main entry point for the bot.

    Initializes the bot and starts polling for messages.
    """
    logger.info("Starting Bridge Bot...")
    if UVLOOP_AVAILABLE:
        logger.info("Using uvloop for better async performance")
    else:
        logger.info("Using default asyncio event loop (install uvloop for better performance)")
    
    # Validate configuration
    if API_ID == 0:
        raise ValueError(
            "API_ID is not set. Please set the API_ID in .env file or environment variable."
        )
    if not API_HASH:
        raise ValueError(
            "API_HASH is not set. Please set the API_HASH in .env file or environment variable."
        )
    if not BOT_TOKEN:
        raise ValueError(
            "BOT_TOKEN is not set. Please set the BOT_TOKEN in .env file or environment variable."
        )
    if ADMIN_CHAT_ID == 0:
        raise ValueError(
            "ADMIN_CHAT_ID is not set. Please set the ADMIN_CHAT_ID in .env file or environment variable."
        )
    
    # Connect to Redis
    try:
        await block_manager.connect()
    except Exception as e:
        logger.warning(f"Failed to connect to Redis: {e}")
        logger.warning("Blocking system will not work without Redis")
    
    # Start the bot
    await bot.start(bot_token=BOT_TOKEN)
    
    me = await bot.get_me()
    logger.info(f"Bot started successfully as @{me.username}")
    logger.info(f"Admin chat ID: {ADMIN_CHAT_ID}")
    logger.info(f"Rate limiting: {'Enabled' if RATE_LIMIT_ENABLED else 'Disabled'}")
    if RATE_LIMIT_ENABLED:
        logger.info(f"Rate limit: {RATE_LIMIT_MAX_MESSAGES} messages per {RATE_LIMIT_WINDOW} seconds")
    logger.info(f"Message mapping cache size: {MAX_MESSAGE_MAPPINGS}")
    
    try:
        # Run until disconnected
        await bot.run_until_disconnected()
    finally:
        # Graceful shutdown
        logger.info("Shutting down gracefully...")
        # Disconnect from Redis on shutdown
        await block_manager.disconnect()
        # Clear caches
        message_mapping.clear()
        original_message_mapping.clear()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    import asyncio
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        # Ensure Redis is disconnected
        asyncio.run(block_manager.disconnect())
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        # Ensure Redis is disconnected
        try:
            asyncio.run(block_manager.disconnect())
        except:
            pass
        raise

