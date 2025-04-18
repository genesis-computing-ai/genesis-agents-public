from genesis_bots.core.logging_config import logger
from genesis_bots.core.tools.project_manager import project_manager
from genesis_bots.demo.app.genesis_app import genesis_app

class BotAutonomy:
    """
    Manages autonomous behaviors and decision-making for bots in the system.
    """
    
    def __init__(self, db_adapter=None, llm_api_key_struct=None):
        """
        Initializes the BotAutonomy instance.
        
        Args:
            db_adapter: Database adapter for accessing bot and system data
            llm_api_key_struct: Structure containing LLM API configuration
        """
        self.db_adapter = db_adapter
        self.llm_api_key_struct = llm_api_key_struct
        
    def get_active_bots(self):
        """
        Retrieves list of active bots from the GenesisApp sessions.
        
        Returns:
            list: List of active bot IDs
        """
        try:
            if genesis_app.sessions:
                return [session.bot_id for session in genesis_app.sessions]
            return []
        except Exception as e:
            logger.error(f"Error retrieving active bots from sessions: {e}")
            return []

    def check_bot_todos(self, bot_id):
        """
        Checks outstanding todos for a specific bot.
        
        Args:
            bot_id (str): ID of the bot to check
            
        Returns:
            list: List of outstanding todos for the bot
        """
        try:
            # Get all projects to check their todos
            projects = project_manager.manage_projects(
                action="LIST",
                bot_id=bot_id
            )
            
            all_todos = []
            for project in projects:
                project_todos = project_manager.get_project_todos(
                    bot_id=bot_id,
                    project_id=project['project_id']
                )
                
                # Filter for todos assigned to this bot that aren't completed or cancelled
                bot_todos = [
                    todo for todo in project_todos 
                    if (todo.get('assigned_to_bot_id') == bot_id and 
                        todo.get('status') not in ['COMPLETED', 'CANCELLED'])
                ]
                
                all_todos.extend(bot_todos)
            
            if all_todos:
                logger.info(f"Bot {bot_id} has {len(all_todos)} outstanding todos")
                for todo in all_todos:
                    logger.info(f"  - TODO {todo['todo_id']}: {todo['todo_name']} (Status: {todo['status']})")
            
            return all_todos
            
        except Exception as e:
            logger.error(f"Error checking todos for bot {bot_id}: {e}")
            return []
        
    def process_autonomous_actions(self):
        """
        Main processing loop for autonomous bot actions.
        This method is called periodically to check and execute any needed autonomous behaviors.
        """
        try:
            logger.info("Processing autonomous bot actions...")
            
            # Get list of active bots from sessions
            active_bots = self.get_active_bots()
            logger.info(f"Found {len(active_bots)} active bots in sessions")
            
            # Check todos for each active bot
            for bot_id in active_bots:
                self.check_bot_todos(bot_id)
                
            # Future implementation will include:
            # - Processing decision trees
            # - Managing autonomous interactions
            # - Taking action on todos based on dependencies and priority
            
        except Exception as e:
            logger.error(f"Error in autonomous processing: {e}")
            raise
