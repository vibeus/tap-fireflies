import backoff
import requests
import time
import singer

from requests.exceptions import ConnectionError, Timeout
from singer import metrics, utils
from simplejson.scanner import JSONDecodeError

LOGGER = singer.get_logger()

FIREFLIES_LIMIT_PER_MINUTE = 60
REQUEST_TIMEOUT = 300

# Credit: refer to tap-intercom: https://github.com/singer-io/tap-intercom/blob/master/tap_intercom/client.py

class Server5xxError(Exception):
    pass

class Server429Error(Exception):
    pass

class FirefliesError(Exception):
    pass

class FirefliesBadRequestError(FirefliesError):
    pass

class FirefliesBadResponseError(FirefliesError):
    pass

class FirefliesInvalidArgumentError(FirefliesError):
    pass

class FirefliesObjectNotFoundError(FirefliesError):
    pass

class FirefliesForbiddenError(FirefliesError):
    pass

class FirefliesPaidRequiredError(FirefliesError):
    pass

class FirefliesNotInTeamError(FirefliesError):
    pass

class FirefliesRequireElevatedPriviledgeError(FirefliesError):
    pass

class FirefliesAccountCancelledError(FirefliesError):
    pass

class FirefliesArgumentRequiredError(FirefliesError):
    pass

class FirefliesRateLimitError(Server429Error):
    pass

class FirefliesRequestTimeoutError(FirefliesError):
    pass

class FirefliesInvalidLanguageCodeError(FirefliesError):
    pass

# barely used (still included tho)
class FirefliesPayloadTooSmallError(FirefliesError):
    pass

class FirefliesAdminMustExistError(FirefliesError):
    pass

# Error codes: https://docs.fireflies.ai/miscellaneous/error-codes
ERROR_CODE_EXCEPTION_MAPPING = {
    "invalid_arguments": {
        "raise_exception": FirefliesInvalidArgumentError,
        "message": "The variables in the GraphQL request is incorrect."
    },
    "object_not_found": {
        "raise_exception": FirefliesObjectNotFoundError,
        "message": "We did not find the object for your query."        
    },
    "forbidden": {
        "raise_exception": FirefliesForbiddenError,
        "message": "You don't have access to perform this action."        
    },
    "paid_required": {
        "raise_exception": FirefliesPaidRequiredError,
        "message": "Consider upgrading your plan to perform this action."        
    },
    "not_in_team": {
        "raise_exception": FirefliesNotInTeamError,
        "message": "You do not have permissions for this team."        
    },
    "require_elevated_privilege": {
        "raise_exception": FirefliesRequireElevatedPriviledgeError,
        "message": "You do not have permission to perform this action."        
    },
    "account_cancelled": {
        "raise_exception": FirefliesAccountCancelledError,
        "message": "Your account is inactive. If this is not expected, please contact support."        
    },
    "args_required": {
        "raise_exception": FirefliesArgumentRequiredError,
        "message": "You must provide the missing argument(s)."        
    },
    "too_many_requests": {
        "raise_exception": FirefliesRateLimitError,
        "message": "Too many requests. Please retry at a later time."        
    },
    "payload_too_small": {
        "raise_exception": FirefliesPayloadTooSmallError,
        "message": "Content size is too small. Please upload files larger than 50kb."        
    },
    "request_timeout": {
        "raise_exception": FirefliesRequestTimeoutError,
        "message": "Request timed out. Please try again or contact support."        
    },
    "invalid_language_code": {
        "raise_exception": FirefliesInvalidLanguageCodeError,
        "message": "Language code is invalid or not supported. Please refer to API docs for supported languages."        
    },
    "admin_must_exist": {
        "raise_exception": FirefliesAdminMustExistError,
        "message": "You must have at least one admin your team."        
    }
}

def get_exception_for_error_code(fireflies_error_status, fireflies_error_code):
    """Maps the error code to the respective error message."""
    exception = ERROR_CODE_EXCEPTION_MAPPING.get(fireflies_error_code, {}).get("raise_exception")
    if not exception:
        exception = Server5xxError if fireflies_error_status >= 500 else FirefliesError
    return exception

def raise_for_error(response):
    """Raises error class with appropriate message for the response."""
    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    errors = response_json.get("errors", [])
    fireflies_error_status = 502
    fireflies_error_code = ""

    if errors: # Response containing `errors` object
        if len(errors) > 1:
            message = "Fireflies-error_status: {}, Error: {}".format(fireflies_error_status, errors)
        else:
            error_message = errors[0].get("message")
            fireflies_error_code = errors[0].get("code")
            fireflies_error_status = errors[0].get("extensions", {}).get("status", 502)
            message = "Fireflies-error_status: {}, Error: {}, Error_Code: {}".format(fireflies_error_status, error_message, fireflies_error_code)
    else:
        # default message
        message = "Fireflies-error_status: {}, Error: {}".format(fireflies_error_status, response_json)
    
    formatted_function = get_exception_for_error_code(fireflies_error_status=fireflies_error_status,
                                                      fireflies_error_code=fireflies_error_code)
    
    raise formatted_function(message) from None


def get_default_header(token):
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

class FirefliesClient:
    def __init__(self, access_token, config_request_timeout):
        """
            endpoint_url: Your GraphQL endpoint. 
            token: token for making requests
        """
        self.base_url = "https://api.fireflies.ai/graphql"
        self.__access_token = access_token
        self.__session = requests.Session()

        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0","" or not passed then it set default to 300 seconds.

        if config_request_timeout and float(config_request_timeout):
            self.__request_timeout = float(config_request_timeout)
        else:
            self.__request_timeout = REQUEST_TIMEOUT                

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    # Rate limiting:
    # https://docs.fireflies.ai/fundamentals/limits
    @backoff.on_exception(backoff.expo, Timeout, max_tries=5, factor=2) # Backoff for request timeout
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, FirefliesBadResponseError, FirefliesRateLimitError),
                          max_tries=4,
                          factor=3)
    @utils.ratelimit(1000, 60)    
    def request(self, method, path=None, url=None, **kwargs):
        if not url and not path:
            url = self.base_url

        LOGGER.info("URL: {} {}, Params: {}, JSON Body: {}".format(method, url, kwargs.get("params"), kwargs.get("json")))

        if "headers" not in kwargs:
            kwargs["headers"] = {}
        
        kwargs["headers"]["Authorization"] = "Bearer {token}".format(token=self.__access_token)

        if method == "POST":
            kwargs["headers"]["Content-Type"] = "application/json"

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, timeout=self.__request_timeout, **kwargs) # Pass request timeout
            timer.tags[metrics.Tag.http_status_code] = response.status_code                

        if response.status_code != 200:
            raise_for_error(response)

        # Sometimes a 200 status code is returned with no content, which breaks JSON decoding.
        try:
            return response.json()
        except JSONDecodeError as err:
            raise FirefliesBadResponseError from err
    
    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)    

    def execute(self, query, variables=None):
        """
            query: the query that we want to execute
            variables: variables in the query if any
        """
        data = {
            "query": query,
            "variables": variables
        }
        json_response = None
        try:
            response = requests.post(self.endpoint, json=data, headers=self.headers)
            response.raise_for_status()
            json_response = response.json()
            if "errors" in json_response and json_response["errors"]:
                print(json_response)
                raise ValueError("GraphQL endpoint returns an error: {json_error}".format(json_error=json_response["errors"]))
            return json_response
        except requests.exceptions.RequestException as request_error:
            LOGGER.error("Meet a Request error, might due to network issues: {e}".format(e=request_error))
        except ValueError as graphql_error:
            LOGGER.error("Meet an error from the API: {e}".format(e=graphql_error))
        except Exception as unexpected_error:
            LOGGER.error("Meet an unexpected error from the API: {e}".format(e=unexpected_error))
