from abc import abstractmethod
from datetime import datetime, timedelta
import json
import pandas as pd
from genesis_bots.core.logging_config import logger

class BotOsRemindersBase:
    @abstractmethod
    def __init__(self, callback_action_function):
        pass

    @abstractmethod
    def add_reminder(self, text, due_date, is_recurring=False, frequency=None, completion_action=None, thread_id="") -> dict:
        pass

    @abstractmethod
    def check_reminders(self, current_time:datetime) -> list:
        pass

    @abstractmethod
    def mark_reminder_completed(self, reminder_id:str):
        pass

class RemindersTest(BotOsRemindersBase):
    def __init__(self, callback_action_function):
        self.callback_action_function = callback_action_function
        self.columns = ['id', 'text', 'due_date', 'is_recurring', 'frequency', 'next_due_date', 'completion_action', 'thread_id']
        self.reminders = None
        self.next_id = 1

    def add_reminder(self, text, due_date, is_recurring=False, frequency=None, completion_action=None, thread_id="") -> dict:
        next_due_date = self.calculate_next_due_date(due_date, frequency) if is_recurring else None
        reminder = {
            'id': str(self.next_id),
            'text': text,
            'due_date': due_date,
            'is_recurring': is_recurring,
            'frequency': frequency,
            'next_due_date': next_due_date,
            'completion_action': json.loads(completion_action) if completion_action else None,
            'thread_id': thread_id
        }
        if self.reminders is not None:
            self.reminders = pd.concat([self.reminders, pd.DataFrame([reminder], columns=self.columns)])
        else:
            self.reminders = pd.DataFrame([reminder], columns=self.columns)
        self.next_id += 1
        return reminder

    def mark_reminder_completed(self, reminder_id:str):
        if self.reminders is None:
            return
        reminder_row = self.reminders[self.reminders['id'] == reminder_id]
        if not reminder_row.empty:
            action = reminder_row.iloc[0]['completion_action']
            if action and action['type'] == 'callback':
                self.callback_action_function(action.get('message', ''))
            self.reminders = self.reminders[self.reminders['id'] != reminder_id]
            logger.info(f"Marked reminder {reminder_id} as completed.")
        else:
            logger.info("Reminder not found.")

    def calculate_next_due_date(self, due_date, frequency):
        if frequency == 'every minute':
            return due_date + timedelta(minutes=1)
        if frequency == 'every 5 minutes':
            return due_date + timedelta(minutes=5)
        if frequency == 'every 15 minutes':
            return due_date + timedelta(minutes=15)
        if frequency == 'hourly':
            return due_date + timedelta(hours=1)
        if frequency == 'daily':
            return due_date + timedelta(days=1)
        elif frequency == 'weekly':
            return due_date + timedelta(weeks=1)
        return None

    def check_reminders(self, current_time:datetime) -> list:
        if self.reminders is None:
            return []
        due_reminders = self.reminders[(self.reminders['due_date'] <= current_time) | ((self.reminders['is_recurring']) & (self.reminders['next_due_date'] <= current_time))]
        for _, reminder in due_reminders.iterrows():
            logger.info(f"Reminder due: {reminder['text']}")
            # Optionally, prepare reminder for completion or notify user/system
        # Return due reminders for further processing if necessary
        return due_reminders.to_dict('records')

