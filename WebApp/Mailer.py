import os
import requests

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")

def send_email(to: str, subject: str, body: str):
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "from": EMAIL_FROM,
                "to": [to],
                "subject": subject,
                "text": body
            },
            timeout=10
        )

        if r.status_code != 200:
            print("Resend error:", r.text)

    except Exception as e:
        print("Email send error:", e)