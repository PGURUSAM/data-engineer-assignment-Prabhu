import os
from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env

import logging
import smtplib
from email.mime.text import MIMEText

def send_alert_email(subject: str, message: str):
    """Send an alert email in case of ETL failure."""
    host = os.getenv("ALERT_EMAIL_HOST")
    port = int(os.getenv("ALERT_EMAIL_PORT", "587"))
    user = os.getenv("ALERT_EMAIL_USER")
    password = os.getenv("ALERT_EMAIL_PASSWORD")
    from_addr = os.getenv("ALERT_EMAIL_FROM")
    to_addr = os.getenv("ALERT_EMAIL_TO")

    if not all([host, port, user, password, from_addr, to_addr]):
        logging.warning("Alert email environment variables not set properly.")
        return

    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, password)
            server.sendmail(from_addr, to_addr, msg.as_string())
        logging.info("Alert email sent.")
    except Exception as e:
        logging.error(f"Failed to send alert email: {e}")