"""
__author__: Ryan Tang
Module: mixins
------------------
This module provides reusable mixin classes to extend the functionality of other classes.
Mixins are designed to be inherited alongside other base classes to add specific features
without affecting the primary inheritance hierarchy.

Classes:
    IncludeLoggerMixin: 
        A mixin that provides lazy-initialized, class-specific logging functionality.
        When inherited, it enables any class to access a logger via the `_logger` property.
        The logger is configured with customizable log level and format, and ensures that
        multiple handlers are not added to the logger instance. This mixin is useful for
        adding consistent logging capabilities across different classes in an application.
"""
import logging
from typing import ClassVar


class IncludeLoggerMixin:
    """
    This mixin lazy initializes a logger for the class, configured with a default log level
    and format. The logger is accessible via the `_logger` property.
    Attributes:
        LOGGER_LEVEL (str): The default logging level for the logger. Defaults to "INFO".
        LOGGER_FORMAT (str): The format string for log messages. Defaults to 
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s".
    Properties:
        _logger (logging.Logger): A lazily initialized logger instance specific to the class.
    Usage:
        - Inherit from this mixin in your class to enable logging functionality.
        - Use `self._logger` to log messages within your class methods.
    Example:
        ```
        class MyClass(IncludeLoggerMixin):
            LOGGER_LEVEL = "ERROR" # Can be used to set the level of the logger
        
            def do_something(self):
                self._logger.info("Doing something!")
        obj = MyClass()
        obj.do_something()
        ```
    """
    # Class-level default configuration
    LOGGER_LEVEL: ClassVar[str] = "INFO"
    LOGGER_FORMAT: ClassVar[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @property
    def _logger(self) -> logging.Logger:
        if not hasattr(self, '_logger_instance'):
            self._logger_instance = logging.getLogger(self.__class__.__name__)
            # Prevent Multiple handlers
            if not self._logger_instance.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(self.LOGGER_FORMAT)
                handler.setFormatter(formatter)
                self._logger_instance.addHandler(handler)
                self._logger_instance.setLevel(self.LOGGER_LEVEL.upper())  # Set log level for logger
                self._logger_instance.info("Logger initialized for %s, with log level: %s", self.__class__.__name__, self.LOGGER_LEVEL.upper())
                # Configure logger here
        return self._logger_instance