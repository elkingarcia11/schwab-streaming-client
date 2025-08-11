#!/usr/bin/env python3
"""
Email Sender for Daily Data Summary
Waits until 4:05 PM EST and sends the entire data/ folder to specified email
"""

import os
import sys
import time
import zipfile
from datetime import datetime, time as dt_time
import pytz
from pathlib import Path

# Add email-client to path
sys.path.append('email-client')
from email_client import EmailClient


def wait_until_4_05_pm_est():
    """
    Efficiently wait until 4:05 PM EST, checking time every 30 seconds
    """
    est_tz = pytz.timezone('US/Eastern')
    
    print("⏰ Waiting until 4:05 PM EST to send daily data summary...")
    
    while True:
        # Get current time in EST
        now_est = datetime.now(est_tz)
        target_time = now_est.replace(hour=16, minute=5, second=0, microsecond=0)
        
        # If it's already past 4:05 PM today, wait until tomorrow
        if now_est.time() >= dt_time(16, 5):
            target_time = target_time.replace(day=target_time.day + 1)
        
        # Calculate time difference
        time_diff = target_time - now_est
        total_seconds = int(time_diff.total_seconds())
        
        if total_seconds <= 0:
            print("🎯 It's 4:05 PM EST! Time to send the email.")
            break
        
        # Calculate hours, minutes, seconds
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        print(f"⏳ Time until 4:05 PM EST: {hours:02d}:{minutes:02d}:{seconds:02d}")
        
        # Sleep for 30 seconds (more efficient than checking every second)
        time.sleep(30)


def create_data_folder_zip():
    """
    Create a zip file of the entire data/ folder
    
    Returns:
        str: Path to the created zip file
    """
    data_folder = Path('data')
    if not data_folder.exists():
        print("❌ data/ folder not found!")
        return None
    
    # Create zip filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_filename = f"data_folder_{timestamp}.zip"
    
    print(f"📦 Creating zip file: {zip_filename}")
    
    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(data_folder):
                for file in files:
                    file_path = Path(root) / file
                    # Add file to zip with relative path
                    arcname = file_path.relative_to(Path('.'))
                    zipf.write(file_path, arcname)
                    print(f"  📄 Added: {arcname}")
        
        print(f"✅ Zip file created successfully: {zip_filename}")
        return zip_filename
        
    except Exception as e:
        print(f"❌ Error creating zip file: {e}")
        return None


def send_daily_data_email(zip_filename):
    """
    Send the data folder zip file to elkizain@gmail.com
    
    Args:
        zip_filename: Path to the zip file to send
    """
    try:
        # Initialize email client
        email_client = EmailClient()
        
        # Email details
        recipient_email = "elkinzain@gmail.com"
        subject = f"Daily Trading Data Summary - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Email body
        body = f"""
📊 Daily Trading Data Summary

📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} EST
📦 Attachment: {zip_filename}

This email contains the complete data folder from today's trading session, including:
- Trade logs (open/closed trades)
- Market data files
- Configuration files
- Any other data generated during the session

Best regards,
Trading System
"""
        
        # Send email with attachment
        print(f"📧 Sending email to {recipient_email}...")
        success = email_client.send_email(
            to_email=recipient_email,
            subject=subject,
            body=body,
            attachment_path=zip_filename
        )
        
        if success:
            print("✅ Email sent successfully!")
        else:
            print("❌ Failed to send email")
            
        return success
        
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False


def cleanup_zip_file(zip_filename):
    """
    Clean up the temporary zip file
    
    Args:
        zip_filename: Path to the zip file to delete
    """
    try:
        if zip_filename and os.path.exists(zip_filename):
            os.remove(zip_filename)
            print(f"🗑️ Cleaned up temporary file: {zip_filename}")
    except Exception as e:
        print(f"⚠️ Warning: Could not clean up {zip_filename}: {e}")


def main():
    """
    Main function: Wait until 4:05 PM EST, send data folder, then exit
    """
    print("🚀 Daily Data Email Sender Started")
    print("=" * 50)
    
    try:
        # Wait until 4:05 PM EST
        wait_until_4_05_pm_est()
        
        # Create zip file of data folder
        zip_filename = create_data_folder_zip()
        if not zip_filename:
            print("❌ Failed to create zip file. Exiting.")
            return
        
        # Send email
        email_sent = send_daily_data_email(zip_filename)
        
        # Clean up
        cleanup_zip_file(zip_filename)
        
        if email_sent:
            print("🎉 Daily data summary sent successfully!")
        else:
            print("❌ Failed to send daily data summary")
            
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        print("👋 Program finished. Exiting.")


if __name__ == "__main__":
    main()