# TODO Create gracious error pages to handle upload errors

"""
Centralized custom exception definitions for the Event Management System.

Each exception inherits from BaseAppError, which itself extends Werkzeug's
HTTPException, allowing clean integration with Flask's error system and
JSON-formatted API responses.

Domain Groups:
--------------
1. Validation Errors (400)
2. Import Errors (422)
3. Database Errors (404–503)
4. System Errors (500)
"""

from werkzeug.exceptions import HTTPException


class BaseAppError(HTTPException):
    """Root application error, base for all custom exceptions."""
    code = 500
    description = "Application error"

    def __init__(self, message=None, details=None, code=None):
        super().__init__(description=message or self.description)
        self.message = message or self.description
        self.details = details or {}
        if code:
            self.code = code

    def to_dict(self):
        """Serialize error info into a JSON-safe dictionary."""
        return {
            "status": "error",
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details
        }


# ==============================================================================
# 1. VALIDATION ERRORS (HTTP 400)
# ==============================================================================

class ValidationError(BaseAppError):
    code = 400
    description = "Validation error"


class MissingTableError(ValidationError):
    description = "Required table not found in Excel file"


class InvalidFormatError(ValidationError):
    description = "Invalid Excel format or unsupported structure"


# ==============================================================================
# 2. IMPORT ERRORS (HTTP 422)
# ==============================================================================

class ImportParsingError(BaseAppError):
    code = 422
    description = "Failed to parse import file"


class CountryTableError(ImportParsingError):
    description = "Invalid or missing country table"


class CustomXmlError(ImportParsingError):
    description = "Invalid or unreadable embedded XML structure"

# ==============================================================================
# 2A. SERIALIZATION ERRORS (HTTP 422)
# ==============================================================================

class SerializationError(BaseAppError):
    """
    Raised when model-to-dict serialization fails or produces
    invalid data for JSON or MongoDB storage.
    """
    code = 422
    description = "Failed to serialize object"

    def __init__(self, message=None, details=None):
        super().__init__(message or self.description, details, code=self.code)



# ==============================================================================
# 3. DATABASE ERRORS (HTTP 404–503)
# ==============================================================================

class DatabaseConnectionError(BaseAppError):
    code = 503
    description = "Database connection failed"


class DuplicateKeyError(BaseAppError):
    code = 409
    description = "Duplicate record detected"


class RecordNotFoundError(BaseAppError):
    code = 404
    description = "Requested record not found"


# ==============================================================================
# 4. SYSTEM ERRORS (HTTP 500)
# ==============================================================================

class ConfigurationError(BaseAppError):
    code = 500
    description = "Configuration missing or invalid"


class UnexpectedError(BaseAppError):
    code = 500
    description = "Unexpected internal error"
