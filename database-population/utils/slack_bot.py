"""
Slack notification utilities for pipeline alerts.
"""

import requests
from config import config
import logging

logger = logging.getLogger(__name__)

def send_slack_alert(message, channel=None, color="danger"):
    """
    Send an alert to Slack.

    Args:
        message: The alert message to send
        channel: Optional channel override (defaults to config)
        color: Color of the alert border - "danger" (red), "warning" (yellow), or "good" (green)
    """
    try:
        slack_config = config("slack")
        webhook_url = slack_config.get("webhook_url")

        if not webhook_url:
            logger.info("Slack webhook URL not configured, skipping alert")
            return False

        # Use configured channel or override
        channel_name = channel or slack_config.get("channel", "#alerts")

        # Format the message with better visual formatting
        payload = {
            "channel": channel_name,
            "attachments": [
                {
                    "color": color,
                    "title": "Database Pipeline Alert",
                    "text": message,
                    "fields": [
                        {
                            "title": "Environment",
                            "value": slack_config.get("environment", "production"),
                            "short": True,
                        },
                        {
                            "title": "Timestamp",
                            "value": __import__("datetime")
                            .datetime.now()
                            .strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "short": True,
                        },
                    ],
                    "footer": "Data Pipeline Monitor",
                    "ts": __import__("time").time(),
                }
            ],
        }

        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info(f"Slack alert sent successfully: {message[:100]}...")
        return True

    except Exception as e:
        logger.error(f"Failed to send Slack alert: {str(e)}")
        return False


def send_pipeline_success_alert(stats):
    """
    Send a success alert with pipeline statistics.

    Args:
        stats: Dictionary containing pipeline run statistics
    """
    message = f"""✅ Pipeline completed successfully!
    
• Bills updated: {stats.get('bills_updated', 0)}
• Hearings updated: {stats.get('hearings_updated', 0)}
• Topics updated: {stats.get('topics_updated', 0)}
• Total runtime: {stats.get('runtime_seconds', 0):.2f} seconds
    """

    send_slack_alert(message, color="good")


def send_pipeline_failure_alert(error_message, error_traceback=None):
    """
    Send a failure alert with error details.

    Args:
        error_message: The error message
        error_traceback: Optional traceback details
    """
    message = f"""❌ *Pipeline failed!*
    
*Error:* {error_message}
    
*Details:* Check logs for more information."""

    if error_traceback:
        # Truncate traceback if too long (Slack has message limits)
        if len(error_traceback) > 1500:
            error_traceback = error_traceback[:1500] + "... (truncated)"
        message += f"\n\n```\n{error_traceback}\n```"

    send_slack_alert(message, color="danger")
