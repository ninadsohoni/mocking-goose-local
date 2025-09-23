"""Volume operations MCP tools for Databricks Unity Catalog."""

import io
import os
import sys
from typing import Optional

from databricks.sdk import WorkspaceClient
from .utils import sanitize_error_message


def load_volume_tools(mcp_server):
    """Register Volume MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with
    """

    @mcp_server.tool()
    def create_volume(
        catalog_name: str,
        schema_name: str,
        volume_name: str,
        volume_type: str = "MANAGED",
        storage_location: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> dict:
        """Create a new Unity Catalog volume.

        Args:
            catalog_name: Name of the catalog to create the volume in
            schema_name: Name of the schema to create the volume in
            volume_name: Name of the volume to create
            volume_type: Type of volume ("MANAGED" or "EXTERNAL")
            storage_location: Storage location for external volumes (required for EXTERNAL volumes)
            comment: Optional comment describing the volume

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Prepare volume creation parameters
            create_params = {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "name": volume_name,
                "volume_type": volume_type.upper(),
            }

            if comment:
                create_params["comment"] = comment

            if volume_type.upper() == "EXTERNAL":
                if not storage_location:
                    return {
                        "success": False,
                        "error": "storage_location is required for EXTERNAL volumes",
                    }
                create_params["storage_location"] = storage_location

            # Create the volume
            volume = w.volumes.create(**create_params)

            return {
                "success": True,
                "volume": {
                    "name": volume.name,
                    "full_name": volume.full_name,
                    "volume_type": volume.volume_type,
                    "catalog_name": volume.catalog_name,
                    "schema_name": volume.schema_name,
                    "owner": volume.owner,
                    "comment": volume.comment,
                    "storage_location": getattr(volume, "storage_location", None),
                    "created_at": volume.created_at,
                    "updated_at": volume.updated_at,
                },
                "message": f"Volume {catalog_name}.{schema_name}.{volume_name} created successfully",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error creating volume: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def update_volume(
        volume_name: str,
        new_name: Optional[str] = None,
        comment: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> dict:
        """Update an existing Unity Catalog volume.

        Args:
            volume_name: Full volume name in catalog.schema.volume format
            new_name: New name for the volume (optional)
            comment: New comment for the volume (optional)
            owner: New owner for the volume (optional)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Parse volume name
            parts = volume_name.split(".")
            if len(parts) != 3:
                return {
                    "success": False,
                    "error": "Volume name must be in format: catalog.schema.volume",
                }

            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Prepare update parameters
            update_params = {"name": volume_name}

            if new_name:
                update_params["new_name"] = new_name
            if comment is not None:
                update_params["comment"] = comment
            if owner:
                update_params["owner"] = owner

            # Update the volume
            volume = w.volumes.update(**update_params)

            return {
                "success": True,
                "volume": {
                    "name": volume.name,
                    "full_name": volume.full_name,
                    "volume_type": volume.volume_type,
                    "catalog_name": volume.catalog_name,
                    "schema_name": volume.schema_name,
                    "owner": volume.owner,
                    "comment": volume.comment,
                    "storage_location": getattr(volume, "storage_location", None),
                    "updated_at": volume.updated_at,
                },
                "message": f"Volume {volume_name} updated successfully",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error updating volume: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def delete_volume(volume_name: str) -> dict:
        """Delete a Unity Catalog volume.

        Args:
            volume_name: Full volume name in catalog.schema.volume format

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Parse volume name
            parts = volume_name.split(".")
            if len(parts) != 3:
                return {
                    "success": False,
                    "error": "Volume name must be in format: catalog.schema.volume",
                }

            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Delete the volume
            w.volumes.delete(name=volume_name)

            return {
                "success": True,
                "message": f"Volume {volume_name} deleted successfully",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error deleting volume: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def upload_file_to_volume(
        volume_path: str, local_file_path: str, overwrite: bool = False
    ) -> dict:
        """Upload a local file to a Unity Catalog volume.

        Args:
            volume_path: Path within the volume where the file will be stored (e.g., /Volumes/catalog/schema/volume/path/file.txt)
            local_file_path: Path to the local file to upload
            overwrite: Whether to overwrite the file if it already exists

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Validate local file exists
            if not os.path.exists(local_file_path):
                return {
                    "success": False,
                    "error": f"Local file not found: {local_file_path}",
                }

            # Read the local file
            with open(local_file_path, "rb") as f:
                file_bytes = f.read()

            # Upload to volume
            binary_data = io.BytesIO(file_bytes)
            w.files.upload(volume_path, binary_data, overwrite=overwrite)

            # Get file size for confirmation
            file_size = len(file_bytes)

            return {
                "success": True,
                "file_path": volume_path,
                "local_file": local_file_path,
                "size_bytes": file_size,
                "overwritten": overwrite,
                "message": f"File uploaded successfully to {volume_path} ({file_size} bytes)",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error uploading file: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def download_file_from_volume(volume_path: str, local_file_path: str) -> dict:
        """Download a file from a Unity Catalog volume to local filesystem.

        Args:
            volume_path: Path within the volume where the file is stored (e.g., /Volumes/catalog/schema/volume/path/file.txt)
            local_file_path: Local path where the file will be saved

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Download the file
            file_content = w.files.download(volume_path)

            # Create local directory if it doesn't exist
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Write to local file
            with open(local_file_path, "wb") as f:
                f.write(file_content.contents)

            # Get file size for confirmation
            file_size = len(file_content.contents)

            return {
                "success": True,
                "volume_path": volume_path,
                "local_file": local_file_path,
                "size_bytes": file_size,
                "message": f"File downloaded successfully to {local_file_path} ({file_size} bytes)",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error downloading file: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def list_volume_files(volume_path: str, recursive: bool = False) -> dict:
        """List files in a Unity Catalog volume directory.

        Args:
            volume_path: Path within the volume to list (e.g., /Volumes/catalog/schema/volume/path/)
            recursive: Whether to list files recursively in subdirectories

        Returns:
            Dictionary with file listings or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # List files in the volume path
            files = w.files.list(volume_path)

            file_list = []
            directory_list = []

            for file_info in files:
                if file_info.is_dir:
                    directory_list.append(
                        {
                            "name": file_info.name,
                            "path": file_info.path,
                            "type": "directory",
                            "modified_at": file_info.modified_at,
                        }
                    )
                    # If recursive, list contents of subdirectories
                    if recursive:
                        try:
                            sub_result = list_volume_files(file_info.path, recursive=True)
                            if sub_result.get("success"):
                                file_list.extend(sub_result.get("files", []))
                                directory_list.extend(sub_result.get("directories", []))
                        except:
                            # Skip subdirectories that can't be accessed
                            pass
                else:
                    file_list.append(
                        {
                            "name": file_info.name,
                            "path": file_info.path,
                            "type": "file",
                            "size_bytes": file_info.file_size,
                            "modified_at": file_info.modified_at,
                        }
                    )

            return {
                "success": True,
                "volume_path": volume_path,
                "files": file_list,
                "directories": directory_list,
                "file_count": len(file_list),
                "directory_count": len(directory_list),
                "recursive": recursive,
                "message": f"Found {len(file_list)} file(s) and {len(directory_list)} director(ies) in {volume_path}",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error listing volume files: {error_msg}", file=sys.stderr)
            return {
                "success": False,
                "error": f"Error: {error_msg}",
                "files": [],
                "directories": [],
                "file_count": 0,
                "directory_count": 0,
            }

    @mcp_server.tool()
    def delete_volume_file(volume_path: str) -> dict:
        """Delete a file from a Unity Catalog volume.

        Args:
            volume_path: Path within the volume where the file is located (e.g., /Volumes/catalog/schema/volume/path/file.txt)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Delete the file
            w.files.delete(volume_path)

            return {
                "success": True,
                "file_path": volume_path,
                "message": f"File {volume_path} deleted successfully",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error deleting volume file: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def create_volume_directory(volume_path: str) -> dict:
        """Create a directory in a Unity Catalog volume.

        Args:
            volume_path: Path within the volume where the directory will be created (e.g., /Volumes/catalog/schema/volume/path/new_dir/)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Create the directory
            w.files.create_directory(volume_path)

            return {
                "success": True,
                "directory_path": volume_path,
                "message": f"Directory {volume_path} created successfully",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error creating directory: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def get_volume_file_info(volume_path: str) -> dict:
        """Get detailed information about a file in a Unity Catalog volume.

        Args:
            volume_path: Path within the volume where the file is located (e.g., /Volumes/catalog/schema/volume/path/file.txt)

        Returns:
            Dictionary with file information or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Get file information
            file_info = w.files.get_status(volume_path)

            return {
                "success": True,
                "file_info": {
                    "name": file_info.name,
                    "path": file_info.path,
                    "is_directory": file_info.is_dir,
                    "size_bytes": file_info.file_size if not file_info.is_dir else None,
                    "modified_at": file_info.modified_at,
                },
                "message": f"File information retrieved for {volume_path}",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error getting file info: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def copy_volume_file(source_path: str, destination_path: str) -> dict:
        """Copy a file within Unity Catalog volumes or between volumes.

        Args:
            source_path: Source file path (e.g., /Volumes/catalog/schema/volume/source.txt)
            destination_path: Destination file path (e.g., /Volumes/catalog/schema/volume/dest.txt)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # Initialize Databricks SDK
            w = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST"),
                token=os.environ.get("DATABRICKS_TOKEN"),
            )

            # Download source file content
            source_content = w.files.download(source_path)

            # Upload to destination
            binary_data = io.BytesIO(source_content.contents)
            w.files.upload(destination_path, binary_data, overwrite=True)

            file_size = len(source_content.contents)

            return {
                "success": True,
                "source_path": source_path,
                "destination_path": destination_path,
                "size_bytes": file_size,
                "message": f"File copied successfully from {source_path} to {destination_path} ({file_size} bytes)",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error copying file: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}

    @mcp_server.tool()
    def move_volume_file(source_path: str, destination_path: str) -> dict:
        """Move a file within Unity Catalog volumes or between volumes.

        Args:
            source_path: Source file path (e.g., /Volumes/catalog/schema/volume/source.txt)
            destination_path: Destination file path (e.g., /Volumes/catalog/schema/volume/dest.txt)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            # First copy the file
            copy_result = copy_volume_file(source_path, destination_path)
            
            if not copy_result.get("success"):
                return copy_result

            # Then delete the source file
            delete_result = delete_volume_file(source_path)
            
            if not delete_result.get("success"):
                # If delete fails, try to clean up the copied file
                try:
                    delete_volume_file(destination_path)
                except:
                    pass
                return {
                    "success": False,
                    "error": f"Failed to delete source file after copy: {delete_result.get('error')}",
                }

            return {
                "success": True,
                "source_path": source_path,
                "destination_path": destination_path,
                "size_bytes": copy_result.get("size_bytes"),
                "message": f"File moved successfully from {source_path} to {destination_path}",
            }

        except Exception as e:
            error_msg = sanitize_error_message(str(e))
            print(f"❌ Error moving file: {error_msg}", file=sys.stderr)
            return {"success": False, "error": f"Error: {error_msg}"}
