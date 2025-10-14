import dlt
import re

def create_safe_column_name(text):
    """Create SQL-safe column names by removing/replacing special characters"""
    safe_text = re.sub(r'[+/\\?=<>()&|!@#$%^*,.:;`"\'\[\]\{\}]', '_', text)
    safe_text = safe_text.replace(' ', '_')
    safe_text = re.sub(r'_+', '_', safe_text)
    safe_text = safe_text.strip('_')
    safe_text = safe_text.lower()[:40]
    return safe_text

def escape_sql_string(text):
    """Escape single quotes in SQL strings by doubling them"""
    return text.replace("'", "''")

def get_concept_metadata(pipeline):
    """Get all concepts and their value types outside the resource function"""
    with pipeline.sql_client() as client:
        concepts_query = """
        SELECT DISTINCT 
            concept_name,
            CASE 
                WHEN value_coded IS NOT NULL THEN 'coded'
                WHEN value_numeric IS NOT NULL THEN 'numeric' 
                WHEN value_text IS NOT NULL THEN 'text'
                WHEN value_datetime IS NOT NULL THEN 'datetime'
                WHEN value_drug IS NOT NULL THEN 'drug'
                ELSE 'other'
            END as value_type
        FROM sql_to_duckdb_pipeline_data.flat_observations 
        WHERE concept_name IS NOT NULL
        """
        
        concepts_result = client.execute_sql(concepts_query)
        concepts = [(row[0], row[1]) for row in concepts_result]
    
    # Get answers for coded concepts
    coded_concept_answers = {}
    for concept_name, value_type in concepts:
        if value_type == 'coded':
            with pipeline.sql_client() as client:
                answers_query = f"""
                SELECT DISTINCT value_coded_name
                FROM sql_to_duckdb_pipeline_data.flat_observations 
                WHERE concept_name = '{escape_sql_string(concept_name)}' 
                  AND value_coded_name IS NOT NULL
                """
                answers_result = client.execute_sql(answers_query)
                answers = [row[0] for row in answers_result]
                coded_concept_answers[concept_name] = answers
    
    return concepts, coded_concept_answers

@dlt.resource(name="observations_widened", write_disposition="replace")
def create_widened_observations():
    """Create widened columns for all value types"""
    
    pipeline = dlt.pipeline()
    
    # Get metadata outside the yield loop
    concepts, coded_concept_answers = get_concept_metadata(pipeline)
    
    if not concepts:
        print("No concepts found for pivoting")
        return
    
    # Build columns for each concept based on value type
    pivot_columns = []
    
    for concept_name, value_type in concepts:
        safe_concept_name = create_safe_column_name(concept_name)
        escaped_concept_name = escape_sql_string(concept_name)
        
        if value_type == 'coded':
            # Create one-hot columns for each answer
            answers = coded_concept_answers.get(concept_name, [])
            for answer_name in answers:
                safe_answer_name = create_safe_column_name(answer_name)
                escaped_answer_name = escape_sql_string(answer_name)
                column_name = f"{safe_concept_name}_{safe_answer_name}"
                
                pivot_columns.append(
                    f"MAX(CASE WHEN concept_name = '{escaped_concept_name}' AND value_coded_name = '{escaped_answer_name}' THEN 1 ELSE 0 END) AS \"{column_name}\""
                )
        
        elif value_type == 'numeric':
            pivot_columns.append(
                f"MAX(CASE WHEN concept_name = '{escaped_concept_name}' THEN value_numeric END) AS \"{safe_concept_name}_value\""
            )
        
        elif value_type == 'text':
            pivot_columns.append(
                f"MAX(CASE WHEN concept_name = '{escaped_concept_name}' THEN value_text END) AS \"{safe_concept_name}_text\""
            )
        
        elif value_type == 'datetime':
            pivot_columns.append(
                f"MAX(CASE WHEN concept_name = '{escaped_concept_name}' THEN value_datetime END) AS \"{safe_concept_name}_datetime\""
            )
        
        elif value_type == 'drug':
            pivot_columns.append(
                f"MAX(CASE WHEN concept_name = '{escaped_concept_name}' THEN value_drug END) AS \"{safe_concept_name}_drug_id\""
            )
    
    # Add base encounter information
    base_columns = [
        "person_id",
        "encounter_id", 
        "encounter_type_name",
        "visit_date_started",
        "location_name",
        "date_created"
    ]
    
    # Build the final query
    pivot_query = f"""
    SELECT 
        {', '.join(base_columns)},
        {', '.join(pivot_columns)}
    FROM sql_to_duckdb_pipeline_data.flat_observations
    GROUP BY 
        {', '.join(base_columns)}
    """
    print("pivot_query")
    print(pivot_query)

    # Execute query and yield results
    with pipeline.sql_client() as client:
        results = client.execute_sql(pivot_query)
        print(results[0])
        # Since execute_sql returns a list, we need to manually create column names
        # Get column names by running a similar query with LIMIT 0
        column_names = base_columns.copy()
        print("col_name")
        print(pivot_columns)
        for col in pivot_columns:
            # Extract column name from "AS \"column_name\""
            print(col)
            if 'AS' in col:
                col_name = col.split(' AS ')[1].strip().strip('"')
                print(col_name)
                column_names.append(col_name)
                
        # Yield each row with proper column names
        for row in results: 
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(column_names):
                    row_dict[column_names[i]] = value
            yield row_dict
        print(row_dict)

def run_pivoting_transformation():
    """Run the comprehensive pivoting transformation"""
    pipeline = dlt.pipeline(
        pipeline_name="sql_to_duckdb_pipeline",
        destination="duckdb", 
        dataset_name="sql_to_duckdb_pipeline_data"
    )
    
    load_info = pipeline.run(create_widened_observations())
    print("✅ Comprehensive pivoting completed! All value types included.")
    return pipeline

if __name__ == '__main__':
    run_pivoting_transformation()