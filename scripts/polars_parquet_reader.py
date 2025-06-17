"""
__author__: Ryan Tang
This module provides utility functions and classes for common operations

Classes:
---------
- PolarsParquetReader: A utility class for handling Parquet files using Polars. It abstracts tedious I/O operations and provides functionality similar to Spark or Pandas for reading, writing, and scanning Parquet files.
"""
from typing import Literal, Any
import polars as pl
from file_system import FileSystem
from mixins import IncludeLoggerMixin

class PolarsParquetReader(IncludeLoggerMixin):
    """Polars Parquet Reader for S3/Local that helps to remove some of the tedious IO Operations, mimics how Spark/Pandas performs IO Operations"""
    def __init__(self, aws_key_id: str | None = None, aws_secret_access_key: str | None = None, aws_region: str | None= None) -> None:
        self._has_cloud = aws_key_id and aws_secret_access_key and aws_region
        self._storage_options = {
            "aws_access_key_id": aws_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region,
        }

        
        self._fs = FileSystem(aws_key=aws_key_id, aws_secret=aws_secret_access_key, aws_region=aws_region)
    
    def __check_path_suffix(self, path:str) -> str:
        """
        Used to check if a path needs a suffix appended to it. Polars has issues processing if the path does not end with a / if it's partitioned.

        Args:
            path: Path to Check

        Returns:
            Suffix Appended Path if needed, else the original path.
        """
        is_s3_path = self._fs.S3_PREFIX in path
        is_a_dir = False
        if not path.endswith("/"):
            files = self._fs.listdir(path)
            
            if is_s3_path and path[len(self._fs.S3_PREFIX):] in files: # Ignore the own file as it'll be listed in s3
                files.remove(path[len(self._fs.S3_PREFIX):])
            if len(files) > 0: 
                is_a_dir = True
            
        return path + "/" if is_a_dir else path
    
    def read_parquet(self, path: str) -> pl.DataFrame:
        """
        Read a parquet file into a Polars DataFrame.
        
        Args:
            path: Path to the parquet file

        Returns:
            Polars Dataframe
        """
        path_suffix = self.__check_path_suffix(path)
        return pl.read_parquet(path_suffix, storage_options=self._storage_options, allow_missing_columns=True) if self._has_cloud else pl.read_parquet(path_suffix, allow_missing_columns=True)
    
    def scan_parquet(self, path: str) -> pl.LazyFrame:
        """
        Read a parquet fileinto a Polars DataFrame.
        
        Args:
            path: Path to the parquet file

        Returns:
            Polars Dataframe
        """
        path_suffix = self.__check_path_suffix(path)
        return pl.scan_parquet(path_suffix, storage_options=self._storage_options, allow_missing_columns=True) if self._has_cloud else pl.scan_parquet(path_suffix, allow_missing_columns=True)
    
    def __remove_empty_structs(self, schema: Any) -> tuple[Any, bool]:
        """
        Recursively removes empty struct fields from a schema.

        Args:
            schema: Schema of the struct

        Returns:
            (new_schema, is_empty) where is_empty indicates if the resulting schema is empty
        """        
        # Handle List types
        if isinstance(schema, pl.List):
            inner_schema, inner_empty = self.__remove_empty_structs(schema.inner)
            if inner_empty:
                # If the inner type is empty, this whole list is considered empty
                return None, True
            return pl.List(inner_schema), False
        
        # Handle Struct types
        elif isinstance(schema, pl.Struct):
            if not schema.fields:
                # Empty struct found
                return None, True
            
            new_fields = {}
            all_empty = True
            
            # Process each field in the struct
            for field in schema.fields:
                field_type = field.dtype
                field_name = field.name
                new_field_type, is_empty = self.__remove_empty_structs(field_type)
                if not is_empty:
                    new_fields[field_name] = new_field_type
                    all_empty = False
            
            if all_empty:
                return None, True
            
            return pl.Struct(new_fields), False
        
        # Any other type is not empty
        return schema, False

    def __transform_data_removing_empty_structs(self, df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        """
        Transform a DataFrame by removing all empty struct fields.
        """
        new_schema = {}
        columns_to_transform = []
        
        df = df.lazy() if isinstance(df, pl.DataFrame) else df
        
        # Analyze schema and identify columns that need transformation
        for column_name, dtype in df.collect_schema().items():
            new_type, is_empty = self.__remove_empty_structs(dtype)
            if is_empty:
                # Skip columns that are completely empty
                continue
            
            if new_type != dtype:
                # Column needs transformation
                new_schema[column_name] = new_type
                columns_to_transform.append(column_name)
        
        # Apply schema transformations
        for column in columns_to_transform:
            self._logger.info(f"Applying Transformations for {columns_to_transform}")
            df = df.with_columns(
                pl.col(column).cast(new_schema[column]).alias(column)
            )
        
        return df
    
    def write_parquet(self, df: pl.DataFrame | pl.LazyFrame, path: str, upsert_key: list[str] | None = None, compression_method: str = "gzip", partition_cols: list[str] | None = None, overwrite_level: Literal['partition', 'full'] | None = None) -> None:
        """
        Helps to write Polars Parquet in Pyarrow Format (i.e. compatible with Spark, Athena), with additional features.
        Args:
            df: Dataframe to write
            path: S3/Local Path to write to
            upsert_key: If Specified, will perform an upsert instead of replacing everything. Defaults to None.
            compression_method: Compression Method. Defaults to "gzip".
            partition_cols: If specified, files will be partitioned in a directory. Defaults to None.
        """
        # Perform Upsert
        lazy_df = df.lazy()
        
        # Add Struct support for the files
        self._logger.info("Attempting to write to parquet")
        has_existing_data = False
        try:
            if self._fs.exists(path):
                test_if_exists = self.scan_parquet(path).collect_schema() # S3 Sometimes causes issue where the directory exists -> Test if exist loading the schema
                del test_if_exists
                has_existing_data = True
            currentDir = self._fs.getdir(path)
            if not self._fs.exists(currentDir):
                self._fs.mkdir(currentDir)
        except pl.exceptions.ComputeError:
            self._fs.remove_dir(path) # Clean up any remainding file
            has_existing_data = False
        
        df_to_write = lazy_df
        if has_existing_data:
            self._logger.info("Have Existing Data")
            df_existing = self.scan_parquet(path)
            
            # Compare Schemas
            df_existing_schema = df_existing.collect_schema()
            new_df_schema = lazy_df.collect_schema()
            
            # Attempt to adjust schemas
            schemas_match = df_existing_schema == new_df_schema
            try:
                if not schemas_match:
                    self._logger.info("Schema don't match existing, attempt to adjust to existing")
                    adjusted_df = lazy_df.with_columns([
                        pl.col(name).cast(dtype) for name, dtype in df_existing_schema.items()
                    ]) # Test if can be matched
                    new_df_schema= adjusted_df.collect_schema()
                    
                    lazy_df = adjusted_df
                    self._logger.info("Adjusted Schema")
                schemas_match = df_existing_schema == new_df_schema
            except Exception as _:
                new_df_schema = lazy_df.collect_schema()
                self._logger.info("Could not adjust schema")
            
            if overwrite_level == "full":
                self._logger.info("Overwrite Mode set to 'Full', Removing Existing Data")
                self._fs.remove_dir(path)
                df_to_write = lazy_df
            elif overwrite_level == "partition":
                self._logger.info("Overwrite Mode set to 'Partition'")
                if not (partition_cols and len(partition_cols) > 0):
                    raise ValueError("Partition Cols are Empty though Overwrite level was set to Partition")
                self._logger.info(f"Overwrite enabled, overwriting on {overwrite_level} level")
                
                self._logger.info("Removing Affected partitions")
                
                partitions_affected_df: pl.LazyFrame = lazy_df \
                    .select(
                        partition_cols
                    ) \
                    .unique()
                    
                partitions_affected = partitions_affected_df.collect().rows()
                
                self._logger.info(f"{len(partitions_affected)} Partitions Affected")
                # Remove all partitions
                self.__remove_partitions(path, partition_cols, partitions_affected)
                
                # update existing df
                df_existing = self.scan_parquet(path)
                
                # if schemas don't match, rewrite whole partition
                if not schemas_match:
                    self._logger.info("Schemas do not match, rewriting whole parquet")
                    df_to_write = pl.concat([lazy_df, df_existing], how="diagonal_relaxed").collect().lazy()
                    self._fs.remove_dir(path)
                else:                        
                    df_to_write = lazy_df
            elif upsert_key and len(upsert_key) > 0:
                self._logger.info("Upserting with key %s", upsert_key)
                if partition_cols and len(partition_cols) > 0 and schemas_match:
                    self._logger.info("Partitions Given, will only update Affected Partitions")
                    partitions_affected_df: pl.LazyFrame = lazy_df \
                        .select(
                            partition_cols
                        ) \
                        .unique()
                        
                    partitions_affected = partitions_affected_df.collect().rows()
                    
                    affected_df = df_existing \
                        .join(
                            partitions_affected_df,
                            on = partition_cols
                        ) \
                        .join(
                            lazy_df,
                            on=upsert_key,
                            how="anti"
                        ) \
                        .collect() \
                        .lazy()
                        
                    self.__remove_partitions(path, partition_cols=partition_cols, partitions_affected=partitions_affected)
                    
                    df_to_write = pl.concat([lazy_df, affected_df], how="diagonal_relaxed")
                else:
                    self._logger.info("Updating Whole Parquet")
                    unaffected_df = df_existing \
                        .join(
                            lazy_df,
                            on = upsert_key,
                            how="anti"
                        )                            
                        
                    df_to_write = pl.concat([lazy_df, unaffected_df], how="diagonal_relaxed").collect().lazy()
                    
                    self._fs.remove_dir(path)
        
        # Problem statement: Sometimes there might be an empty struct field in the schema, which causes issues when saving to parquet, hence strip all the empty struct fields
        transformed_df = self.__transform_data_removing_empty_structs(df_to_write)
        df_to_write = transformed_df.collect()
        
        self._logger.info("Performing Write Operations")
        if partition_cols is not None and len(partition_cols) > 0:
            df_to_write.write_parquet(
                path,
                compression=compression_method, # type: ignore
                storage_options=self._storage_options,
                use_pyarrow=True,
                pyarrow_options={"partition_cols": partition_cols}
            )
        else:
            df_to_write.write_parquet(
                path,
                compression=compression_method, # type: ignore
                storage_options=self._storage_options,
                use_pyarrow=True
            )
        
        self._logger.info(f"Finished Writing to parquet {path}")

    def __remove_partitions(self, path:str, partition_cols: list[str], partitions_affected: list[tuple]) -> None:
        self._logger.info("Removing Affected Partitions and Updating")
        for partition in partitions_affected:
            partition_path = f"{path}/" + "/".join([f"{col}={partition[index]}" for index, col in enumerate(partition_cols)])
            if self._fs.exists(partition_path):
                self._fs.remove_dir(partition_path)
                self._logger.debug(f"Removing Partition Path: {partition_path}")