"""Constants for the Whisker Ting integration."""

DOMAIN = "whisker_ting"

# AWS Cognito Configuration
COGNITO_REGION = "us-east-1"
COGNITO_USER_POOL_ID = "us-east-1_trW4gH661"
COGNITO_CLIENT_ID = "4akjeqt9gtl8rgg1cksunipk9u"

# API Configuration
API_BASE_URL = "https://api.wskr.io"
API_USERS_ENDPOINT = "/api/v1/Users/{user_id}"
API_FROZEN_PIPE_STATUS_ENDPOINT = "/api/v1/FrozenPipe/{serial_number}"
API_FROZEN_PIPE_HISTORY_ENDPOINT = "/api/v1/FrozenPipe/{serial_number}/currentHistory"
API_NOTIFICATION_HISTORY_ENDPOINT = "/api/v1/Notifications/history/{user_id}"

# SignalR WebSocket
SIGNALR_URL = "wss://signalr.api.wskr.io/dataHub"

# Update interval
DEFAULT_SCAN_INTERVAL = 60  # seconds
MIN_SCAN_INTERVAL = 30  # seconds
MAX_SCAN_INTERVAL = 3600  # seconds (1 hour)

# Config keys
CONF_USERNAME = "username"
CONF_PASSWORD = "password"
CONF_USER_ID = "user_id"
CONF_API_KEY = "api_key"
CONF_ACCESS_TOKEN = "access_token"
CONF_REFRESH_TOKEN = "refresh_token"
CONF_ID_TOKEN = "id_token"
CONF_SCAN_INTERVAL = "scan_interval"

# Device types
DEVICE_TYPE_FIRE_SENSOR = "FireSensor"

# Hazard status values
HAZARD_STATUS_NO_HAZARD = "No Hazards Detected"
HAZARD_STATUS_REVIEWED_NOT_FIRE = "ReviewedNotFire"

# Platforms
PLATFORMS = ["binary_sensor", "sensor"]
