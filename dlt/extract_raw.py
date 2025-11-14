import dlt
from dlt.sources.sql_database import sql_database

def load_tables():
	"""Extract raw data from SQL database and load into DuckDB using dlt"""
	source = sql_database().with_resources(
		"person",
		"person_name",
		"person_address",
		"person_attribute",
		"patient",
		"patient_identifier",
		"patient_identifier_type",
		"encounter",
		"encounter_type",
		"encounter_provider",
		"encounter_role",
		"obs",
		"visit",
		"visit_type",
		"location",
		"provider",
		"concept",
		"concept_name",
		"concept_answer",
		"concept_class",
		"concept_datatype",
		"concept_set",
		"program",
		"program_workflow",
		"program_workflow_state",
		"patient_program",
		"patient_state",
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
		"global_property"
	)

    # specify different loading strategy for each resource using apply_hints
 	# Core patient and encounter data
	source.person.apply_hints(
		write_disposition="merge",
		primary_key="person_id",
		incremental=dlt.sources.incremental("date_created")
	)
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
	source.person_attribute.apply_hints(
		write_disposition="merge",
		primary_key="person_attribute_id",
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
	source.concept_answer.apply_hints(
		write_disposition="merge",
		primary_key="concept_answer_id",
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
		incremental=dlt.sources.incremental("date_created")
	)
	source.patient_state.apply_hints(
		write_disposition="merge",
		primary_key="patient_state_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Patient identifiers
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

	# Encounter providers and roles
	source.encounter_provider.apply_hints(
		write_disposition="merge",
		primary_key="encounter_provider_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.encounter_role.apply_hints(
		write_disposition="merge",
		primary_key="encounter_role_id",
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
	source.drug_order.apply_hints(
		write_disposition="merge",
		primary_key="order_id"
		# Note: drug_order is a child table of orders, no date_created column
	)

	# Relationships
	source.relationship.apply_hints(
		write_disposition="merge",
		primary_key="relationship_id",
		incremental=dlt.sources.incremental("date_created")
	)

	# Users and roles
	source.users.apply_hints(
		write_disposition="merge",
		primary_key="user_id",
		incremental=dlt.sources.incremental("date_created")
	)
	source.user_role.apply_hints(
		write_disposition="merge",
		primary_key="user_id"  # Composite key (user_id, role), no timestamp columns
	)
	source.role_privilege.apply_hints(
		write_disposition="merge",
		primary_key="role"  # Composite key (role, privilege), no timestamp columns
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
	source.form_resource.apply_hints(
		write_disposition="merge",
		primary_key="form_resource_id"
		# Note: date_changed can be NULL, so we don't use incremental loading
	)

	# Global properties
	source.global_property.apply_hints(
		write_disposition="merge",
		primary_key="property"
		# Note: date_changed can be NULL, so we don't use incremental loading for this table
	)

	# Create a dlt pipeline object
	pipeline = dlt.pipeline(
		pipeline_name="openmrs_etl", # Custom name for the pipeline
		destination=dlt.destinations.duckdb("/opt/airflow/data/openmrs_etl.duckdb"),
		dataset_name="openmrs_analytics" # Custom name for the dataset created in the destination
	)

	# Run the pipeline
	#load_info = pipeline.run(source, write_disposition="append")
	load_info = pipeline.run(source)

	# Pretty print load information
	print(load_info)

if __name__ == '__main__':
	load_tables()
