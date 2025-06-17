"""
__author__: Ryan Tang

file_system.py
This module provides a utility class `FileSystem` for handling file operations both locally and on S3. 
It includes methods for reading, writing, managing metadata, and performing file system operations.

Classes:
---------
- FileSystem: A utility class for handling file operations both locally and on S3, including reading, 
    writing, and metadata management.
"""

import s3fs
import os
import logging
import boto3
from typing import List
from mixins import IncludeLoggerMixin

class FileSystem(IncludeLoggerMixin):
    """
    A utility class for handling file operations both locally and on S3, including reading, writing, and metadata management.
    """
    _fs = None
    S3_PREFIX = r"s3://"
    
    def __init__(self, aws_key: str | None = None, aws_secret: str | None = None, aws_region: str | None= None) -> None:
        """
        Can Create a specific file system with certain credentials for S3. Uses app.config credentials by default

        Args:
            aws_key: AWS Key.
            aws_secret: AWS Secret.
            aws_region: AWS Region.
        """
        self._aws_key = aws_key
        self._aws_secret = aws_secret
        self._aws_region = aws_region
    
    def __get_s3fs(self) -> s3fs.S3FileSystem:
        if not self._aws_key or not self._aws_secret or not self._aws_region:
            raise ValueError("Missing AWS Configurations, Reinitialise with Configurations")

        if self._fs is None:
            self._fs = s3fs.S3FileSystem(
            key=self._aws_key,
            secret=self._aws_secret,
            client_kwargs={'region_name': self._aws_region})
        
        return self._fs

    def open(self, file_path:str, mode="r"):
        """
        Opens a file either locally or from S3 depending on the path 

        Args:
            filepath: Path to File
            mode: Process to open the file. Defaults to "r".
        """
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.open(file_path, mode)
        
        # Traditional need to check if folder exists, else create if don't exists
        parent_dir = os.path.dirname(file_path)
        if parent_dir.strip() != '' and not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        return open(file_path, mode)
    
    def update_s3_content_type(self, bucket_name:str, object_key:str, new_content_type:str, new_content_disposition:str) -> None:
        """
        Updates the s3 Content Type and Content Disposition of an object.

        Args:
            bucket_name: Name of the s3 bucket
            object_key: key of the object to update
            new_content_type: Content Type to update
            new_content_disposition: Content Disposition
        """
        s3 = boto3.client('s3')
        copy_source = {'Bucket': bucket_name, 'Key': object_key}
        s3.copy_object(
            Bucket=bucket_name,
            Key=object_key,
            CopySource=copy_source,
            ContentType=new_content_type,
            ContentDisposition=new_content_disposition,
            MetadataDirective='REPLACE'
        )
            
    
    def chmod(self, file_path:str, acl:str) -> None:
        """
        Used to change the permissions of a file or directory.

        Args:
            file_path: Path to the file or directory
            acl: Access Control List (ACL) to set. For example, "public-read" for S3.
        """
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.chmod(file_path, acl)
        
        # Traditional need to check if folder exists, else create if don't exists
        return os.chmod(file_path, acl)
    
    def exists(self, file_path:str) -> bool:
        """
        Checks if a file or directory exists.

        Args:
            file_path: Path of the file or directory to check

        Returns:
            Whether the file or directory exists
        """
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.exists(file_path)
        
        return os.path.exists(file_path)
    
    def parent(self, file_path:str) -> str:
        """
        Gets the Parent of the file

        Args:
            file_path: Path of file or directory to check

        Returns:
            str: Parent Directory
        """
        from pathlib import Path

        path = Path(file_path)
        parent = path.parent
        return str(parent)
    
    def getdir(self, file_path:str) -> str:
        """
        Gets the Directory of the file

        Args:
            file_path: Path of file or directory to check

        Returns:
            str: Directory
        """
        from pathlib import Path

        path = Path(file_path)
        return str(path.parent) if not path.is_dir() else str(path)
    
    def listdir(self, file_path:str) -> List[str]:
        """
        Lists the contents of a directory.

        Args:
            file_path: Path of the directory to list

        Returns:
            Contents of the directory
        """
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.ls(file_path)
        
        if os.path.isfile(file_path):
            return []
        return os.listdir(file_path)
    
    def mkdir(self, file_path:str) -> None:
        """
        Creates a directory.

        Args:
            file_path: Directory path to create
        """
        dir_name = os.path.dirname(file_path)
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.mkdir(dir_name)
        
        
        return os.mkdir(dir_name)
    
    def remove_file(self, file_path:str) -> None:
        """
        Removes a file

        Args:
            file_path: File to Remove
        """
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.rm(file_path)
        
        return os.remove(file_path)
    
    def remove_dir(self, file_path:str) -> None:
        """
        Removes a directory and all its contents.

        Args:
            file_path: Directory to remove
        """
        if file_path.startswith(self.S3_PREFIX):
            fs = self.__get_s3fs()
            return fs.rm(file_path, recursive=True)
        
        # Traditional need to remove all files first
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                for file in os.listdir(file_path):
                    file_full_path = os.path.join(file_path, file)
                    if os.path.isdir(file_full_path):
                        self.remove_dir(file_full_path)
                    else:
                        os.remove(file_full_path)
                return os.rmdir(file_path)
            else:
                os.remove(file_path)