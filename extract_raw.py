import dlt
from dlt.sources.sql_database import sql_database

def load_tables():
	"""Extract raw data from SQL database and load into DuckDB using dlt""" 
	source = sql_database().with_resources(
		"person",
		"patient",
		"encounter",
		"obs",
		"visit",
		"location",
		"provider",
		"concept",
		"concept_name",
		"concept_class",
		"concept_datatype",
		"concept_set",
		"encounter_type",
		"visit_type",
		"program",
		"program_workflow",
		"program_workflow_state",
		"patient_program",
		"patient_state",
		"patient_identifier",
		"patient_identifier_type",
		"person_attribute",
		"orders",
		"drug",
		"drug_order",
		"relationship",
		"users",
		"user_role",
		"role_privilege",
		"form",
		"form_field",
		"form_resource",
		"global_property",
		"person_name",
		"person_address"
	)

    # specify different loading strategy for each resource using apply_hints
 	# Core patient and encounter data
	source.person.apply_hints(
		write_disposition="merge",
		primary_key="person_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.patient.apply_hints(
		write_disposition="merge",
		primary_key="patient_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.encounter.apply_hints(
		write_disposition="merge",
		primary_key="encounter_id",
		incremental=dlt.sources.incremental("encounter_datetime")
	)
	source.obs.apply_hints(
		write_disposition="merge",
		primary_key="obs_id",
		incremental=dlt.sources.incremental("obs_datetime")
	)
	source.visit.apply_hints(
		write_disposition="merge",
		primary_key="visit_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.location.apply_hints(
		write_disposition="merge",
		primary_key="location_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.provider.apply_hints(
		write_disposition="merge",
		primary_key="provider_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Concepts
	source.concept.apply_hints(
		write_disposition="merge",
		primary_key="concept_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.concept_name.apply_hints(
		write_disposition="merge",
		primary_key="concept_name_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.concept_class.apply_hints(
		write_disposition="merge",
		primary_key="concept_class_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.concept_datatype.apply_hints(
		write_disposition="merge",
		primary_key="concept_datatype_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.concept_set.apply_hints(
		write_disposition="merge",
		primary_key="concept_set_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Encounter and visit types
	source.encounter_type.apply_hints(
		write_disposition="merge",
		primary_key="encounter_type_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.visit_type.apply_hints(
		write_disposition="merge",
		primary_key="visit_type_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Programs and workflows
	source.program.apply_hints(
		write_disposition="merge",
		primary_key="program_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.program_workflow.apply_hints(
		write_disposition="merge",
		primary_key="program_workflow_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.program_workflow_state.apply_hints(
		write_disposition="merge",
		primary_key="program_workflow_state_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.patient_program.apply_hints(
		write_disposition="merge",
		primary_key="patient_program_id",
		incremental=dlt.sources.incremental("date_enrolled")
	)
	source.patient_state.apply_hints(
		write_disposition="merge",
		primary_key="patient_state_id",
		incremental=dlt.sources.incremental("start_date")
	)

	# Patient identifiers and attributes
	source.patient_identifier.apply_hints(
		write_disposition="merge",
		primary_key="patient_identifier_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.patient_identifier_type.apply_hints(
		write_disposition="merge",
		primary_key="patient_identifier_type_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.person_attribute.apply_hints(
		write_disposition="merge",
		primary_key="person_attribute_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Orders and drugs
	source.orders.apply_hints(
		write_disposition="merge",
		primary_key="order_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.drug.apply_hints(
		write_disposition="merge",
		primary_key="drug_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Relationships and users
	source.relationship.apply_hints(
		write_disposition="merge",
		primary_key="relationship_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.users.apply_hints(
		write_disposition="merge",
		primary_key="user_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Forms
	source.form.apply_hints(
		write_disposition="merge",
		primary_key="form_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.form_field.apply_hints(
		write_disposition="merge",
		primary_key="form_field_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Misc / support tables
	source.person_name.apply_hints(
		write_disposition="merge",
		primary_key="person_name_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.person_address.apply_hints(
		write_disposition="merge",
		primary_key="person_address_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Create a dlt pipeline object
	pipeline = dlt.pipeline(
		pipeline_name="openmrs_etl", # Custom name for the pipeline
		destination="duckdb", # dlt destination to which the data will be loaded
		dataset_name="openmrs_analytics" # Custom name for the dataset created in the destination
	)

	# Run the pipeline
	#load_info = pipeline.run(source, write_disposition="append")
	load_info = pipeline.run(source)

	# Pretty print load information
	print(load_info)

if __name__ == '__main__':
	load_tables()
