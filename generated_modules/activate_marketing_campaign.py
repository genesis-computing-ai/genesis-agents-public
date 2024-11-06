import json
from core.logging_config import logger

def activate_marketing_campaign(channel, segment_description, when_to_launch, budget):
    # Validate channel
    valid_channels = ['meta', 'snap', 'tradedesk']
    if channel not in valid_channels:
        raise ValueError('Invalid channel. Please choose from \'meta\', \'snap\', or \'tradedesk\'.')
    
    # Define segment validation requirements per channel
    segment_requirements = {
        'meta': ['email_list'], #['demographics', 'interests_behaviors'],
        'snap': ['audience_filters', 'custom_audiences'],
        'tradedesk': ['demographic_targeting', 'behavioral_targeting'],
    }
    
    # Check segment description contains required keys
    try:
        segment_description = json.loads(segment_description)
    except json.JSONDecodeError:
        return 'Segment description must be a valid JSON string.'
    for key in segment_requirements[channel]:
        if key not in segment_description:
            raise ValueError(f'Missing segment requirement for {channel}: {key}')
    
    # Simulate the marketing campaign activation
    campaign_details = {
        'channel': channel,
        'segment_description': segment_description,
        'when_to_launch': when_to_launch,
        'budget': budget,
        'status': 'activated'
    }
    
    return campaign_details

# Test function
def test_activate_marketing_campaign():
    # Setup test parameters
    meta_campaign = {
        'channel': 'meta',
        'segment_description': {'demographics': '18-24', 'interests_behaviors': 'sports'},
        'when_to_launch': '2023-12-25',
        'budget': 5000
    }

    # Activate campaign
    result = activate_marketing_campaign(
        meta_campaign['channel'],
        meta_campaign['segment_description'],
        meta_campaign['when_to_launch'],
        meta_campaign['budget'],
    )
    assert result['status'] == 'activated'
    logger.info('Test passed: Meta Campaign activated successfully.')
    
    # Add more tests for snap and tradedesk as needed

#test_activate_marketing_campaign()
# Start of Generated Description
TOOL_FUNCTION_DESCRIPTION_ACTIVATE_MARKETING_CAMPAIGN = {
    "type": "function",
    "function": {
        "name": "activate_marketing_campaign--activate_marketing_campaign",
        "description": "Activates a marketing campaign based on the specified channel, segment description, launch time, and budget.",
        "parameters": {
            "type": "object",
            "properties": {
                "channel": {
                    "type": "string",
                    "description": "The channel to activate the campaign on (valid options: 'meta', 'snap', 'tradedesk')."
                },
                "segment_description": {
                    "type": "string",
                    "description": "json-encoded dict describing the segment relevant to the channel being used for the campaign. for meta, specify email_list as a list of email addresses for the segment"
                },
                "when_to_launch": {
                    "type": "string",
                    "description": "The date and time the campaign should be launched."
                },
                "budget": {
                    "type": "string",
                    "description": "The budget allocated for the campaign."
                }
            },
            "required": [
                "channel",
                "segment_description",
                "when_to_launch",
                "budget"
            ]
        }
    }
}
# End of Generated Description

# Start of Generated Mapping
activate_marketing_campaign_action_function_mapping = {'activate_marketing_campaign--activate_marketing_campaign': activate_marketing_campaign}
# End of Generated Mapping
