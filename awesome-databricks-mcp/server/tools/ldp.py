import os
import sys
import base64

from enum import Enum
from typing_extensions import TypedDict
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines, workspace


def load_ldp_tools(mcp_server):
  """Register Lakeflow Declarative Pipeline MCP tools with the server.

  Args:
      mcp_server: The FastMCP server instance to register tools with
  """
  class TableType(Enum):
    MATERIALIZED_VIEW = "materialized_view"
    STREAMING_TABLE = "streaming_table"


  class TableMedallionType(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

  class QualityExpectationAction(Enum):
    DROP = "drop"
    ALLOW = "allow"

    @staticmethod
    def action_handler(action):
      if action == QualityExpectationAction.DROP:
        return "ON VIOLATION DROP ROW"
      else:
        return ""

    

  class QualityExpectation(TypedDict):
    sql_rule: str
    name: str
    action: QualityExpectationAction

  class Transformation(TypedDict):
    table_name: str
    table_type: TableType
    table_medallion_type: TableMedallionType
    QualityExpectations: list[QualityExpectation]
    sql: str

  @mcp_server.tool()
  def delete_ldp_pipeline_logic(
    pipeline_name: str
  ) -> dict:
    """
      Delete the logic of a Lakeflow Declarative Pipeline.

      Do this to clear out any irrelevant logic or tables from the pipeline before uploading new logic.
      If you're unsure if there's already logic associated with the pipeline, assume it already exists and delete it.
    """
    try:
      w = WorkspaceClient(
          host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
        )

      # Get the current authenticated user's home folder
      current_user = w.current_user.me()
      user_name = current_user.user_name
      user_home = f"/Workspace/Users/{user_name}"

      # Build the base pipeline folder path
      pipeline_base = f"{user_home}/{pipeline_name}/transformations"

      try:
        w.workspace.delete(path=pipeline_base, recursive=True)
      except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" not in str(e):
          raise e
      return {
        "success": True,
        "message": f"Pipeline logic deleted successfully for {pipeline_name}",
      }
    except Exception as e:
      print(f"❌ Error deleting pipeline logic for {pipeline_name}: {str(e)}", file=sys.stderr)
      return {
        "success": False,
        "error": f"Failed to delete pipeline logic for {pipeline_name}: {str(e)}"
      }

  @mcp_server.tool()
  def update_ldp_pipeline_logic(
    pipeline_name: str,
    transformations: list[Transformation]
  ) -> dict:
    """Initialise or update the logic of a Lakeflow Declarative Pipeline.

    Args:
        pipeline_name: The name of the pipeline to update
        transformations: A list of transformation definitions. Each transformation should include:
            - table_name (str): Name of the table to be created.
            - table_type (Enum[TableType]): The type of table (e.g., MATERIALIZED_VIEW, STREAMING_TABLE). Use STREAMING_TABLE for bronze or append-only tables, and MATERIALIZED_VIEWs for anything else.
            - table_medallion_type (Enum[TableMedallionType]): The medallion layer (bronze, silver, gold).
            - sql (str): The SQL logic for the transformation.
            - quality_expectations: A list of quality expectations for the transformation.
              Each quality expectation should include:
              - sql_rule (str): The SQL rule for the quality expectation. This is a boolean exression.
              - name (str): The name of the quality expectation.
              - action (Enum[QualityExpectationAction]): The action to take if the quality expectation is violated.

    Returns:
        dict: A dictionary indicating success or failure, and details about the pipeline update process.
            Example:
            {
                "success": True,
                "message": "Pipeline logic updated successfully.",
                "pipeline_id": "<pipeline_id>"
            }
            On failure, returns:
            {
                "success": False,
                "error": "<error message>"
            }

    Notes:
        - This tool updates the SQL logic for the specified pipeline.
        - It does not execute the pipeline or create Databricks jobs.
        - All operations are performed in the authenticated user's workspace area.
        - Table creation SQL is generated in the format:
            CREATE <TABLE_TYPE> <TABLE_NAME> AS <SQL>
        - Only Databricks-compatible SQL syntax should be used in the transformation SQL.

    **IMPORTANT** Guidance:
      - If a Bronze transformation is extracting data from a CSV, 
        it should be a STREAMING_TABLE and the query should be of the form:
        ```
        SELECT *
        FROM STREAM READ_FILES(/Volumes/<catalog>/<schema>/<volume>/<folder_path>", format => "csv", inferSchema => true, header => true)
        ```
        There should be no quality expectations for a Bronze transformation.
      - A Silver transformation should include Quality expectations.
    """

    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Get the current authenticated user's home folder
      current_user = w.current_user.me()
      user_name = current_user.user_name
      user_home = f"/Workspace/Users/{user_name}"

      # Build the base pipeline folder path
      pipeline_base = f"{user_home}/{pipeline_name}/transformations"

      # Define medallion folders
      medallion_folders = ["bronze", "silver", "gold"]

      # Create the folders in the workspace
      for medallion in medallion_folders:
        folder_path = f"{pipeline_base}/{medallion}"
        try:
          w.workspace.mkdirs(path=folder_path)
        except Exception as e:
          # If the folder already exists, ignore; otherwise, raise
          if "RESOURCE_ALREADY_EXISTS" not in str(e):
            raise e
    
      # For each transformation, create a SQL file and upload it to the respective medallion folder
      for transformation in transformations:
        table_name = transformation["table_name"]
        medallion_type = transformation["table_medallion_type"].value.lower()

        sql_file_path = f"{pipeline_base}/{medallion_type}/{table_name}.sql"

        expectation_sql = ",\n".join([
          f"CONSTRAINT {expectation['name']} EXPECT ({expectation['sql_rule']}) {QualityExpectationAction.action_handler(expectation['action'])}" 
          for expectation in transformation["QualityExpectations"]
        ])
        # Upload the SQL logic as a file to the workspace
        sql_logic = f"CREATE {transformation['table_type'].value.upper().replace('_', ' ')} {table_name} AS {transformation['sql']}"
        try:
          w.workspace.import_(
            path=sql_file_path,
            content=base64.b64encode(sql_logic.encode()).decode(),
            format = workspace.ImportFormat.SOURCE,
            language = workspace.Language.SQL,
            overwrite=True
          )
        except Exception as e:
          print(f"❌ Error uploading SQL file for {table_name}: {str(e)}", file=sys.stderr)
          return {
            "success": False,
            "error": f"Failed to upload SQL file for {table_name}: {str(e)}"
          }

      return {
        "success": True,
        "message": f"Pipeline logic updated successfully for {pipeline_name}",
      }

    except Exception as e:
      print(f"❌ Error updating pipeline logic for {pipeline_name}: {str(e)}", file=sys.stderr)
      return {
        "success": False,
        "error": f"Failed to update pipeline logic for {pipeline_name}: {str(e)}"
      }



      # Get the pipeline
  


  @mcp_server.tool()
  def build_ldp_pipeline(
    name: str,
    catalog: str,
    schema: str
  ) -> dict:
    """Build a Lakeflow Declarative Pipeline in Databricks.

    This tool creates the pipeline itself, but requires the logic is already defined and uploaded to the workspace.
    If a pipeline with the same name already exists, it will be deleted and replaced with the new pipeline.
    See or use the tool update_ldp_pipeline_logic for more details.
    
    Args:
        name (str): The name of the pipeline. This will be used as the base folder in the user's workspace.
        catalog (str): The Unity Catalog catalog where the pipeline's tables will reside.
        schema (str): The schema within the catalog for the pipeline's tables.

    Returns:
        dict: A dictionary indicating success or failure, and details about the pipeline creation process.
            Example:
            {
                "success": True,
                "pipeline_id": "<pipeline_id>",
                "pipeline_name": "<pipeline_name>",
                "message": "Pipeline created successfully with ID <pipeline_id>",
                "replaced_existing": True/False
            }
            On failure, returns:
            {
                "success": False,
                "error": "<error message>"
            }

    Notes:
        - This tool creates the pipeline itself, but requires the logic is already defined and uploaded to the workspace.
        - If a pipeline with the same name exists, it will be automatically deleted and replaced.
        - It does not execute the pipeline or create Databricks jobs.
        - All operations are performed in the authenticated user's workspace area.
        - Table creation SQL is generated in the format:
            CREATE <TABLE_TYPE> <TABLE_NAME> AS <SQL>
        - Only Databricks-compatible SQL syntax should be used in the transformation SQL.
    
    """

    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Get the current authenticated user's home folder
      current_user = w.current_user.me()
      user_name = current_user.user_name
      user_home = f"/Workspace/Users/{user_name}"

      # Build the base pipeline folder path
      pipeline_base = f"{user_home}/{name}/transformations"

      # Check if a pipeline with the same name already exists
      existing_pipeline_id = None
      replaced_existing = False
      
      try:
        # List all pipelines to find if one with the same name exists
        pipelines_list = w.pipelines.list_pipelines()
        for pipeline in pipelines_list:
          if pipeline.name == name:
            existing_pipeline_id = pipeline.pipeline_id
            print(f"🔍 Found existing pipeline '{name}' with ID {existing_pipeline_id}. Will delete and replace.", file=sys.stderr)
            break
        
        # If existing pipeline found, delete it
        if existing_pipeline_id:
          try:
            w.pipelines.delete(existing_pipeline_id)
            replaced_existing = True
            print(f"🗑️ Successfully deleted existing pipeline '{name}' with ID {existing_pipeline_id}", file=sys.stderr)
          except Exception as delete_error:
            print(f"⚠️ Warning: Failed to delete existing pipeline {existing_pipeline_id}: {str(delete_error)}", file=sys.stderr)
            # Continue with creation anyway - the create might still work
            
      except Exception as list_error:
        print(f"⚠️ Warning: Could not check for existing pipelines: {str(list_error)}. Proceeding with creation.", file=sys.stderr)
      
      # Create the new pipeline
      pipeline = w.pipelines.create(
        name=name,
        schema=schema,
        catalog=catalog,
        libraries=[pipelines.PipelineLibrary(glob=pipelines.PathPattern(include=f"{pipeline_base}/**"))],
        serverless=True,
        development=True
      )

      message = f'Pipeline {name} created successfully with ID {pipeline.pipeline_id}'
      if replaced_existing:
        message += f' (replaced existing pipeline)'

      return {
        'success': True,
        'pipeline_id': pipeline.pipeline_id,
        'pipeline_name': name,
        'message': message,
        'replaced_existing': replaced_existing,
      }

    except Exception as e:
      print(f"❌ Error building Lakeflow Declarative Pipeline {name}: {str(e)}", file=sys.stderr)
      return {
        "success": False,
        "error": f"Failed to build Lakeflow Declarative Pipeline {name}: {str(e)}"
      }

  @mcp_server.tool()
  def get_pipeline_errors(pipeline_id: str) -> dict:
    """Get events of a specific lakeflow delcarative pipeline run.
    Use this to examine the reason a pipeline run failed with error.
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )
      # Get pipeline run events
      events = w.pipelines.list_pipeline_events(pipeline_id)

      def process_error(event):
        return {
          'event_id': event.id,
          'event_type': event.event_type,
          'fatal': event.error.fatal,
          'message': event.message,
          'timestamp': event.timestamp,
          'exceptions': [
            {
              'message': exception.message,
              'class_name': exception.class_name,
            }
            for exception in event.error.exceptions
          ]
        }

      return {
        'success': True,
        'events': [process_error(e) for e in events if e.error is not None],
      }
    except Exception as e:
      print(f'❌ Error getting pipeline run events: {str(e)}', file=sys.stderr)
      return {'success': False, 'error': f'Error: {str(e)}'}

  @mcp_server.tool()
  def get_pipeline_run(pipeline_id: str, update_id) -> dict:
    """Get details of a specific lakeflow delcarative pipeline run.
    Use this to check for success or failure of a pipeline run. This can be used to check for table refreshes as well.

    Args:
        pipeline_id: The ID of the pipeline to get details for
        update_id: The ID of the update to get details for

    Returns:
        Dictionary with pipeline run details or error message
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Get pipeline run details
      run = w.pipelines.get_update(pipeline_id, update_id)

      return {
        'success': True,
        'run': {
          'update_id': run.update.update_id,
          'pipeline_id': run.update.pipeline_id,
          'state': run.update.state,
          'creation_time': run.update.creation_time,
        },
        'message': f'Pipeline run {update_id} details retrieved successfully',
      }

    except Exception as e:
      print(f'❌ Error getting pipeline run details: {str(e)}', file=sys.stderr)
      return {'success': False, 'error': f'Error: {str(e)}'}

  @mcp_server.tool()
  def start_pipeline_update(pipeline_id: str, parameters: dict = None) -> dict:
    """Start a lakeflow delcarative pipeline update.

    Args:
        pipeline_id: The ID of the pipeline to start
        parameters: Optional parameters for the pipeline update

    Returns:
        Dictionary with operation result or error message
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Start pipeline update
      run = w.pipelines.start_update(
        pipeline_id=pipeline_id
      )

      return {
        'success': True,
        'pipeline_id': pipeline_id,
        'update_id': run.update_id,
        'message': f'Pipeline update started successfully with update ID {run.update_id}',
      }

    except Exception as e:
      print(f'❌ Error starting pipeline update: {str(e)}', file=sys.stderr)
      return {'success': False, 'error': f'Error: {str(e)}'}

  @mcp_server.tool()
  def stop_pipeline_update(pipeline_id: str) -> dict:
    """Stop a running lakeflow delcarative pipeline update.

    Args:
        pipeline_id: The ID of the pipeline to stop

    Returns:
        Dictionary with operation result or error message
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Stop pipeline update
      w.pipelines.stop_update(pipeline_id)

      return {
        'success': True,
        'pipeline_id': pipeline_id,
        'message': f'Pipeline update stopped successfully for {pipeline_id}',
      }

    except Exception as e:
      print(f'❌ Error stopping pipeline update: {str(e)}', file=sys.stderr)
      return {'success': False, 'error': f'Error: {str(e)}'}

  pass
