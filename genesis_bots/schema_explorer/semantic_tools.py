
import yaml, time, random, string
from genesis_bots.core.logging_config import logger

def create_empty_semantic_model(model_name="", model_description=""):
    # Define the basic structure of the semantic model with an empty tables list
    semantic_model = {
        'model_name': model_name,
        'description': model_description,  # Description is left empty to be filled later
        'tables': []  # Initialize with an empty list of tables
    }
    return semantic_model

# Usage of the function

def convert_model_to_yaml(json_model):
    """
    Convert the JSON representation of the semantic model to YAML format.

    Args:
        json_model (dict): The semantic model in JSON format.

    Returns:
        str: The semantic model in YAML format.
    """
    try:
        yaml_model = yaml.dump(json_model, default_flow_style=False, sort_keys=False)
        return yaml_model
    except yaml.YAMLError as exc:
        logger.info(f"Error converting JSON to YAML: {exc}")
        return None

def convert_yaml_to_json(yaml_model):
    """
    Convert the YAML representation of the semantic model to JSON format.

    Args:
        yaml_model (str): The semantic model in YAML format.

    Returns:
        dict: The semantic model in JSON format, or None if conversion fails.
    """
    try:
        json_model = yaml.safe_load(yaml_model)
        return json_model
    except yaml.YAMLError as exc:
        logger.info(f"Error converting YAML to JSON: {exc}")
        return None


def modify_semantic_model(semantic_model, command, parameters):
    # Validate the command
    valid_commands = [
        'add_table', 'remove_table', 'update_table', 'add_dimension', 'update_dimension', 'remove_dimension',
        'add_time_dimension', 'remove_time_dimension', 'update_time_dimension',
        'add_measure', 'remove_measure', 'update_measure', 'add_filter', 'remove_filter', 'update_filter',
        'set_model_name', 'set_model_description', 'help'
    ]
    if command not in valid_commands:
        return {"success": False, "message": "Invalid command provided. Valid commands are: " + ", ".join(valid_commands)}

    if command == 'help':

        help_message = """
        The following commands are available to modify the semantic model:

        - 'add_table': Adds a new table to the semantic model.
            Parameters: 'table_name', 'database', 'schema', 'table', 'description' (optional).
        - 'remove_table': Removes an existing table from the semantic model.
            Parameters: 'table_name'.
        - 'update_table': Updates an existing table's details in the semantic model.
            Parameters: 'table_name', 'new_values' (a dictionary with any of 'name', 'description', 'database', 'schema', 'table').
        - 'add_dimension': Adds a new dimension to an existing table.
            Parameters: 'table_name', 'dimension_name', 'expr', 'description' (optional), 'synonyms' (optional, list), 'unique' (optional, boolean), 'sample_values' (optional, list).
        - 'update_dimension': Updates an existing dimension in a table.
            Parameters: 'table_name', 'dimension_name', 'new_values' (a dictionary with any of 'name', 'expr', 'description', 'synonyms', 'unique', 'sample_values').
        - 'remove_dimension': Removes an existing dimension from a table.
            Parameters: 'table_name', 'dimension_name'.
        - 'add_time_dimension': Adds a new time dimension to an existing table.
            Parameters: 'table_name', 'time_dimension_name', 'expr', 'description' (optional), 'synonyms' (optional, list), 'unique' (optional, boolean), 'sample_values' (optional, list).
        - 'remove_time_dimension': Removes an existing time dimension from a table.
            Parameters: 'table_name', 'time_dimension_name'.
        - 'update_time_dimension': Updates an existing time dimension in a table.
            Parameters: 'table_name', 'time_dimension_name', 'new_values' (a dictionary with any of 'name', 'expr', 'description', 'synonyms', 'unique', 'sample_values').
        - 'add_measure': Adds a new measure to an existing table.
            Parameters: 'table_name', 'measure_name', 'expr', 'description' (optional), 'synonyms' (optional, list), 'unique' (optional, boolean), 'sample_values' (optional, list), 'default_aggregation' (optional).
        - 'remove_measure': Removes an existing measure from a table.
            Parameters: 'table_name', 'measure_name'.
        - 'update_measure': Updates an existing measure in a table.
            Parameters: 'table_name', 'measure_name', 'new_values' (a dictionary with any of 'name', 'expr', 'description', 'synonyms', 'unique', 'sample_values', 'default_aggregation').
        - 'add_filter': Adds a new filter to an existing table.
            Parameters: 'table_name', 'filter_name', 'expr', 'description' (optional), 'synonyms' (optional, list).
        - 'remove_filter': Removes an existing filter from a table.
            Parameters: 'table_name', 'filter_name'.
        - 'update_filter': Updates an existing filter in a table.
            Parameters: 'table_name', 'filter_name', 'new_values' (a dictionary with any of 'name', 'expr', 'description', 'synonyms').
        - 'set_model_name': Sets the name of the semantic model.
            Parameters: 'model_name'.
        - 'set_model_description': Sets the description of the semantic model.
            Parameters: 'model_description'.

        Note that all "expr" must be SQL-executable expressions that could work as part of a SELECT clause (for dimension and measures, often just the physical column name) or WHERE clause (for filters).
        """
        return {"success": True, "message": help_message}


    try:
        if command == 'set_model_name':
            semantic_model['model_name'] = parameters.get('model_name', '')
            return {"success": True, "message": f"Model name set to '{semantic_model['model_name']}'.", "semantic_yaml": semantic_model}

        if command == 'set_model_description':
            semantic_model['description'] = parameters.get('model_description', '')
            return {"success": True, "message": f"Model description set to '{semantic_model['description']}'.", "semantic_yaml": semantic_model}

        if 'table_name' not in parameters:
            return {"success": False, "message": "Missing parameter 'table_name'.", "semantic_yaml": semantic_model}
        table_name = parameters['table_name']
        table = next((table for table in semantic_model.get('tables', []) if table['name'] == table_name), None)

        if command in ['remove_table', 'add_table', 'update_table'] and not table and command != 'add_table':
            return {"success": False, "message": f"Table '{table_name}' not found."}

        if command == 'add_table':
            required_physical_table_keys = ['database', 'schema', 'table']
            if not all(key in parameters for key in required_physical_table_keys):
                missing_keys = [key for key in required_physical_table_keys if key not in parameters]
                return {"success": False, "message": f"Missing physical table parameters: {', '.join(missing_keys)}."}

            if table:
                return {"success": False, "message": f"Table '{table_name}' already exists.", "semantic_yaml": semantic_model}

            new_table = {
                'name': table_name,
                'description': parameters.get('description', ''),
                'physical_table': {
                    'database': parameters['database'],
                    'schema': parameters['schema'],
                    'table': parameters['table']
                },
                'dimensions': [],
                'time_dimensions': [],
                'measures': [],
                'filters': []
            }
            semantic_model.setdefault('tables', []).append(new_table)
            return {"success": True, "message": f"Table '{table_name}' added.", "semantic_yaml": semantic_model}

        elif command == 'remove_table':
            semantic_model['tables'] = [t for t in semantic_model['tables'] if t['name'] != table_name]
            return {"success": True, "message": f"Table '{table_name}' removed."}

        elif command == 'update_table':
            if not table:
                return {"success": False, "message": f"Table '{table_name}' not found."}
            new_values = parameters.get('new_values', {})
            for key, value in new_values.items():
                if key in table:
                    table[key] = value
            if 'database' in parameters or 'schema' in parameters or 'table' in parameters:
                table['physical_table'] = {
                    'database': parameters.get('database', table['physical_table']['database']),
                    'schema': parameters.get('schema', table['physical_table']['schema']),
                    'table': parameters.get('table', table['physical_table']['table'])
                }
            description = parameters.get('description')
            if description:
                table['description'] = description
            return {"success": True, "message": f"Table '{table_name}' updated.", "semantic_yaml": semantic_model}

        elif 'dimension_name' in parameters or 'measure_name' in parameters or 'filter_name' in parameters or 'time_dimension_name' in parameters:
            if not table:
                return {"success": False, "message": f"Table '{table_name}' not found."}

            item_key = ('time_dimension_name' if 'time_dimension_name' in parameters else
                        'dimension_name' if 'dimension_name' in parameters else
                        'measure_name' if 'measure_name' in parameters else
                        'filter_name' if 'filter_name' in parameters else
                        None)
            item_name = parameters[item_key]
            item_list = table.get('time_dimensions' if 'time_dimension' in command else 'dimensions' if 'dimension' in command else
                                  'measures' if 'measure' in command else
                                  'filters', [])
            item = next((i for i in item_list if i['name'] == item_name), None)
            if command.startswith('remove') and not item:
                return {"success": False, "message": f"{item_key[:-5].capitalize()} '{item_name}' not found in table '{table_name}'."}

            if command.startswith('add'):
                if item:
                    return {"success": False, "message": f"{item_key[:-5].capitalize()} '{item_name}' already exists in table '{table_name}'.", "semantic_yaml": semantic_model}
                expr = parameters.get('expr')
                if expr is None:
                    return {"success": False, "message": f"Expression parameter 'expr' for {item_key[:-5].capitalize()} '{item_name}' is required.", "semantic_yaml": semantic_model}
                new_item = {'name': item_name, 'expr': expr}
                description = parameters.get('description')
                if description:
                    new_item['description'] = description
                synonyms = parameters.get('synonyms', [])
                if synonyms:
                    new_item['synonyms'] = synonyms
                unique = parameters.get('unique', None)
                if unique is not None:
                    new_item['unique'] = unique
                if 'measure' in command:
                    default_aggregation = parameters.get('default_aggregation')
                    if default_aggregation:
                        new_item['default_aggregation'] = default_aggregation
                if 'filter' not in command:
                    sample_values = parameters.get('sample_values', [])
                    if sample_values:
                        new_item['sample_values'] = sample_values
                item_list.append(new_item)
                return {"success": True, "message": f"{item_key[:-5].capitalize()} '{item_name}' added to table '{table_name}'.", "semantic_yaml": semantic_model}

            elif command.startswith('update'):
                if not item:
                    return {"success": False, "message": f"{item_key[:-5].capitalize()} '{item_name}' not found in table '{table_name}'.", "semantic_yaml": semantic_model}
                new_values = parameters.get('new_values', {})
                if 'expr' in new_values:
                    expr = new_values.pop('expr')
                    if expr is not None:
                        item['expr'] = expr
                if 'unique' in new_values:
                    unique = new_values.pop('unique')
                    if isinstance(unique, bool):
                        item['unique'] = unique
                if 'measure' in command:
                    default_aggregation = new_values.pop('default_aggregation', None)
                    if default_aggregation is not None:
                        item['default_aggregation'] = default_aggregation
                if 'filter' not in command:
                    sample_values = new_values.pop('sample_values', None)
                    if sample_values is not None:
                        item['sample_values'] = sample_values
                item.update(new_values)
                description = parameters.get('description')
                if description:
                    item['description'] = description
                synonyms = parameters.get('synonyms')
                if synonyms is not None:
                    item['synonyms'] = synonyms
                return {"success": True, "message": f"{item_key[:-5].capitalize()} '{item_name}' updated in table '{table_name}'.", "semantic_yaml": semantic_model}
            elif command.startswith('remove'):
                table[item_key[:-6] + 's'] = [i for i in item_list if i['name'] != item_name]
                return {"success": True, "message": f"{item_key[:-5].capitalize()} '{item_name}' removed from table '{table_name}'.", "semantic_yaml": semantic_model}
    except KeyError as e:
        return {"success": False, "message": f"Missing necessary parameter '{e.args[0]}'."}
    except Exception as e:
        return {"success": False, "message": f"An unexpected error occurred: {e}"}



def test_modify_semantic_model(semantic_model):
    from genesis_bots.schema_explorer.semantic_tools import modify_semantic_model
    def random_string(prefix, length=5):
        return prefix + '_' + ''.join(random.choices(string.ascii_lowercase, k=length))

    num_tables = random.randint(2, 5)
    tables = [random_string('table') for _ in range(num_tables)]

    model_name = random_string('model')
    model_description = random_string('description', 10)
    semantic_model = modify_semantic_model(semantic_model, 'set_model_name', {'model_name': model_name})
    semantic_model = semantic_model.get('semantic_yaml')
    semantic_model = modify_semantic_model(semantic_model, 'set_model_description', {'model_description': model_description})
    semantic_model = semantic_model.get('semantic_yaml')


    for table_name in tables:
        database_name = random_string('database')
        schema_name = random_string('schema')
        physical_table_name = random_string('physical_table')
        semantic_model = modify_semantic_model(semantic_model, 'add_table', {
            'table_name': table_name,
            'database': database_name,
            'schema': schema_name,
            'table': physical_table_name
        })
        semantic_model = semantic_model.get('semantic_yaml')

    # Add 2-5 random dimensions, measures, and filters to each table
    for table_name in tables:
        for _ in range(random.randint(2, 5)):
            dimension_name = random_string('dimension')
            dimension_description = f"Description for {dimension_name}"
            dimension_expr = random_string('expr', 5)
            synonyms_count = random.randint(0, 3)
            dimension_synonyms = [random_string('synonym') for _ in range(synonyms_count)]
            sample_values_count = random.randint(0, 5)
            dimension_sample_values = [random_string('', random.randint(7, 12)) for _ in range(sample_values_count)]
            semantic_model = modify_semantic_model(semantic_model, 'add_dimension', {
                'table_name': table_name,
                'dimension_name': dimension_name,
                'description': dimension_description,
                'synonyms': dimension_synonyms,
                'unique': False,
                'expr': dimension_expr,
                'sample_values': dimension_sample_values
            })
            semantic_model = semantic_model.get('semantic_yaml')


            time_dimension_name = random_string('time_dimension')
            time_dimension_description = f"Description for {time_dimension_name}"
            time_dimension_expr = random_string('expr', 5)
            time_dimension_synonyms_count = random.randint(0, 3)
            time_dimension_synonyms = [random_string('synonym') for _ in range(time_dimension_synonyms_count)]
            time_dimension_sample_values_count = random.randint(0, 5)
            time_dimension_sample_values = [random_string('', random.randint(7, 12)) for _ in range(time_dimension_sample_values_count)]
            semantic_model = modify_semantic_model(semantic_model, 'add_time_dimension', {
                'table_name': table_name,
                'time_dimension_name': time_dimension_name,
                'description': time_dimension_description,
                'synonyms': time_dimension_synonyms,
                'unique': False,
                'expr': time_dimension_expr,
                'sample_values': time_dimension_sample_values
            })
            semantic_model = semantic_model.get('semantic_yaml')

            measure_name = random_string('measure')
            measure_description = f"Description for {measure_name}"
            measure_expr = random_string('expr', 5)
            measure_synonyms_count = random.randint(0, 2)
            measure_synonyms = [random_string('synonym') for _ in range(measure_synonyms_count)]
            measure_sample_values_count = random.randint(0, 5)
            measure_sample_values = [random_string('', random.randint(7, 12)) for _ in range(measure_sample_values_count)]
            default_aggregations = ['sum', 'avg', 'min', 'max', 'median', 'count', 'count_distinct']
            default_aggregation = random.choice(default_aggregations)
            semantic_model = modify_semantic_model(semantic_model, 'add_measure', {
                'table_name': table_name,
                'measure_name': measure_name,
                'description': measure_description,
                'synonyms': measure_synonyms,
                'unique': False,
                'expr': measure_expr,
                'sample_values': measure_sample_values,
                'default_aggregation': default_aggregation
            })
            semantic_model = semantic_model.get('semantic_yaml')
            filter_name = random_string('filter')
            filter_description = f"Description for {filter_name}"
            filter_expr = random_string('expr', 5)
            filter_synonyms_count = random.randint(0, 2)
            filter_synonyms = [random_string('synonym') for _ in range(filter_synonyms_count)]
            semantic_model = modify_semantic_model(semantic_model, 'add_filter', {
                'table_name': table_name,
                'filter_name': filter_name,
                'description': filter_description,
                'synonyms': filter_synonyms,
                'expr': filter_expr
            })
            semantic_model = semantic_model.get('semantic_yaml')
    if semantic_model is None:
        raise ValueError("Semantic model is None, cannot proceed with modifications.")

    # Update some of the tables, dimensions, measures, and filters
    # TODO: Add update tests for more of the parameters beside these listed below

    updated_table_names = {}
    for table_name in tables:
        if random.choice([True, False]):
            new_table_name = random_string('updated_table')
            result = modify_semantic_model(semantic_model, 'update_table', {'table_name': table_name, 'new_values': {'name': new_table_name}})
            if result.get('success'):
                semantic_model = result.get('semantic_yaml')
                updated_table_names[table_name] = new_table_name
            else:
                raise Exception(f"Error updating table: {result.get('message')}")

    for original_table_name in tables:
        current_table_name = updated_table_names.get(original_table_name, original_table_name)
        if semantic_model and 'tables' in semantic_model:
            table = next((t for t in semantic_model['tables'] if t['name'] == current_table_name), None)
            if table:
                for dimension in table.get('dimensions', []):
                    if random.choice([True, False]):
                        new_dimension_name = random_string('updated_dimension')
                        result = modify_semantic_model(semantic_model, 'update_dimension', {'table_name': current_table_name, 'dimension_name': dimension['name'], 'new_values': {'name': new_dimension_name}})
                        if result.get('success'):
                            semantic_model = result.get('semantic_yaml')
                        else:
                            raise Exception(f"Error updating dimension: {result.get('message')}")

                for measure in table.get('measures', []):
                    if random.choice([True, False]):
                        new_measure_name = random_string('updated_measure')
                        result = modify_semantic_model(semantic_model, 'update_measure', {'table_name': current_table_name, 'measure_name': measure['name'], 'new_values': {'name': new_measure_name}})
                        if result.get('success'):
                            semantic_model = result.get('semantic_yaml')
                        else:
                            raise Exception(f"Error updating measure: {result.get('message')}")

                for filter in table.get('filters', []):
                    if random.choice([True, False]):
                        new_filter_name = random_string('updated_filter')
                        result = modify_semantic_model(semantic_model, 'update_filter', {'table_name': current_table_name, 'filter_name': filter['name'], 'new_values': {'name': new_filter_name}})
                        if result.get('success'):
                            semantic_model = result.get('semantic_yaml')
                        else:
                            raise Exception(f"Error updating filter: {result.get('message')}")

    # Update descriptions for tables, dimensions, measures, and filters using modify_semantic_model
    for table in semantic_model.get('tables', []):
        # Update table description
        if random.choice([True, False]):
            new_description = f"Updated description for {table['name']}"
            result = modify_semantic_model(semantic_model, 'update_table', {'table_name': table['name'], 'new_values': {'description': new_description}})
            if result.get('success'):
                semantic_model = result.get('semantic_yaml')
            else:
                raise Exception(f"Error updating table description: {result.get('message')}")

        # Update dimensions descriptions
        for dimension in table.get('dimensions', []):
            if random.choice([True, False]):
                new_description = f"Updated description for {dimension['name']}"
                result = modify_semantic_model(semantic_model, 'update_dimension', {'table_name': table['name'], 'dimension_name': dimension['name'], 'new_values': {'description': new_description}})
                if result.get('success'):
                    semantic_model = result.get('semantic_yaml')
                else:
                    raise Exception(f"Error updating dimension description: {result.get('message')}")

        # Update measures descriptions
        for measure in table.get('measures', []):
            if random.choice([True, False]):
                new_description = f"Updated description for {measure['name']}"
                result = modify_semantic_model(semantic_model, 'update_measure', {'table_name': table['name'], 'measure_name': measure['name'], 'new_values': {'description': new_description}})
                if result.get('success'):
                    semantic_model = result.get('semantic_yaml')
                else:
                    raise Exception(f"Error updating measure description: {result.get('message')}")

        # Update filters descriptions
        for filter in table.get('filters', []):
            if random.choice([True, False]):
                new_description = f"Updated description for {filter['name']}"
                result = modify_semantic_model(semantic_model, 'update_filter', {'table_name': table['name'], 'filter_name': filter['name'], 'new_values': {'description': new_description}})
                if result.get('success'):
                    semantic_model = result.get('semantic_yaml')
                else:
                    raise Exception(f"Error updating filter description: {result.get('message')}")
    # Verify the re
    # Update the physical table for some of the logical tables
    for table_name in tables:
        current_table_name = updated_table_names.get(table_name, table_name)
        if random.choice([True, False]):  # Randomly decide whether to update the physical table
            new_database_name = random_string('new_database')
            new_schema_name = random_string('new_schema')
            new_physical_table_name = random_string('new_physical_table')
            result = modify_semantic_model(semantic_model, 'update_table', {
                'table_name': current_table_name,
                'new_values': {
                    'physical_table': {
                        'database': new_database_name,
                        'schema': new_schema_name,
                        'table': new_physical_table_name
                    }
                }
            })
            if result.get('success'):
                semantic_model = result.get('semantic_yaml')
                updated_table_names[table_name] = new_physical_table_name  # Track the updated table names
            else:
                raise Exception(f"Error updating physical table: {result.get('message')}")

    assert 'tables' in semantic_model
    assert len(semantic_model['tables']) == num_tables
    for table in semantic_model['tables']:
        if 'dimensions' not in table or not (2 <= len(table['dimensions']) <= 5):
            raise AssertionError("Table '{}' does not have the required number of dimensions (between 2 and 5).".format(table.get('name')))
        assert 'measures' in table and 2 <= len(table['measures']) <= 5
        assert 'filters' in table and 2 <= len(table['filters']) <= 5
    # Check that each table has a physical table with the correct fields set
    for table in semantic_model.get('tables', []):
        physical_table = table.get('physical_table')
        if not physical_table:
            raise Exception(f"Table '{table['name']}' does not have a physical table associated with it.")
        required_fields = ['database', 'schema', 'table']
        for field in required_fields:
            if field not in physical_table or not physical_table[field]:
                raise Exception(f"Physical table for '{table['name']}' does not have the required field '{field}' set correctly.")

    return semantic_model

def suggest_improvements(semantic_model):
    """
    Analyze the semantic model and suggest improvements to make it more comprehensive and complete.

    Args:
        semantic_model (dict): The semantic model in JSON format.

    Returns:
        list: A list of suggestions for improving the semantic model.
    """
    suggestions = []

    # Check if model name and description are set
    if not semantic_model.get('model_name'):
        suggestions.append("Consider adding a 'model_name' to your semantic model for better identification.")
    if not semantic_model.get('description'):
        suggestions.append("Consider adding a 'description' to your semantic model to provide more context.")

    # Check for tables
    tables = semantic_model.get('tables', [])
    if not tables:
        suggestions.append("Your semantic model has no tables. Consider adding some tables to it.")
    else:
        # Check for uniqueness of table names
        table_names = [table.get('name') for table in tables]
        if len(table_names) != len(set(table_names)):
            suggestions.append("Some table names are not unique. Ensure each table has a unique name.")

        synonyms = set()
        synonym_conflicts = set()
        tables_with_synonyms = 0
        tables_with_sample_values = 0

        for table in tables:
            # Check for table description
            if not table.get('description'):
                suggestions.append(f"Table '{table['name']}' has no description. Consider adding a description for clarity.")

            # Check for physical table mapping
            physical_table = table.get('physical_table')
            if not physical_table or not all(key in physical_table for key in ['database', 'schema', 'table']):
                suggestions.append(f"Table '{table['name']}' has incomplete physical table mapping. Ensure 'database', 'schema', and 'table' are defined.")

            # Check for dimensions, measures, and filters
            if not table.get('dimensions'):
                suggestions.append(f"Table '{table['name']}' has no dimensions. Consider adding some dimensions.")
            if not table.get('measures'):
                suggestions.append(f"Table '{table['name']}' has no measures. Consider adding some measures.")
            if not table.get('filters'):
                suggestions.append(f"Table '{table['name']}' has no filters. Consider adding some filters.")

            # Check for time dimensions
            if 'time_dimensions' not in table or not table['time_dimensions']:
                suggestions.append(f"Table '{table['name']}' has no time dimensions. Consider adding time dimensions for time-based analysis.")

            # Check for synonyms and sample_values
            for element in table.get('dimensions', []) + table.get('measures', []) + table.get('filters', []) + table.get('time_dimensions', []):
                if element.get('synonyms'):
                    tables_with_synonyms += 1
                    for synonym in element['synonyms']:
                        if synonym in synonyms:
                            synonym_conflicts.add(synonym)
                        synonyms.add(synonym)

                if 'sample_values' in element and len(element['sample_values']) >= 5:
                    tables_with_sample_values += 1

        # Suggestions for synonyms
        if tables_with_synonyms < len(tables) / 2:
            suggestions.append("Consider adding synonyms to at least half of the dimensions, measures, and filters for better searchability.")

        if synonym_conflicts:
            suggestions.append(f"Synonyms {', '.join(synonym_conflicts)} are not unique across the semantic model. Consider making synonyms unique.")

        # Suggestions for sample_values
        if tables_with_sample_values < len(tables) / 2:
            suggestions.append("Consider adding at least five examples of 'sample_values' on at least half of the measures, dimensions, and time dimensions for better examples in your model.")

    return suggestions



# Test the function
#for _ in range(1):
#    semantic_json = create_empty_semantic_model('Test Data')
#    semantic_json = test_modify_semantic_model(semantic_json)
#    logger.info(convert_model_to_yaml(semantic_json))
#    logger.info('\nSuggestions:')
#    suggestions = suggest_improvements(semantic_json)



# Define a global map to store semantic models by thread_id
semantic_models_map = {}

def initialize_semantic_model(model_name, thread_id):
    """
    Creates an empty semantic model and stores it in a map with the thread_id as the key.

    Args:
        model_name (str): The name of the model to initialize.
        thread_id (str): The unique identifier for the thread.
    """
    # Create an empty semantic model
    empty_model = create_empty_semantic_model(model_name)
    # Store the model in the map using thread_id as the key
    map_key = thread_id + "__" + model_name
    semantic_models_map[thread_id] = empty_model

    if empty_model is not None:
        return {"Success": True, "Message": f"The model {model_name} has been initialized."}
    else:
        return {"Success": False, "Error": "Failed to initialize the model."}

def modify_and_update_semantic_model(model_name, thread_id, modifications):
    """
    Modifies the semantic model based on the provided modifications, updates the model in the map,
    and returns the modified semantic model without the resulting YAML.

    Args:
        model_name (str): The name of the model to modify.
        thread_id (str): The unique identifier for the thread.
        modifications (dict): The modifications to apply to the semantic model.

    Returns:
        dict: The modified semantic model.
    """
    # Construct the map key
    map_key = thread_id + "__" + model_name
    # Retrieve the semantic model from the map
    semantic_model = semantic_models_map.get(map_key)
    if not semantic_model:
        raise ValueError(f"No semantic model found for model_name: {model_name} and thread_id: {thread_id}")

    # Call modify_semantic_model with the retrieved model and the modifications
    result = modify_semantic_model(semantic_model, modifications)

    # Check if 'semantic_yaml' is in the result and store it back into the map
    if 'semantic_yaml' in result:
        semantic_models_map[map_key] = result['semantic_yaml']
        # Strip 'semantic_yaml' parameter from result
        del result['semantic_yaml']
    # Return the modified semantic model without the resulting YAML
    return result


def get_semantic_model(model_name, thread_id):
    """
    Retrieves an existing semantic model from the map based on the model name and thread id.

    Args:
        model_name (str): The name of the model to retrieve.
        thread_id (str): The unique identifier for the thread.

    Returns:
        dict: A JSON wrapper with the semantic model if found, otherwise an error message.
    """
    # Construct the map key
    map_key = thread_id + "__" + model_name
    # Retrieve the semantic model from the map
    semantic_model = semantic_models_map.get(map_key)
    if semantic_model:
        return {"Success": True, "SemanticModel": semantic_model}
    else:
        return {"Success": False, "Error": f"No semantic model found for model_name: {model_name} and thread_id: {thread_id}"}



#logger.info("Test completed successfully.")

#yaml_model = convert_model_to_yaml(semantic_json)
#logger.info(yaml_model)


