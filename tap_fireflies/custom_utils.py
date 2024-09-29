import requests
import time
import singer

FIREFLIES_LIMIT_PER_MINUTE = 60
LOGGER = singer.get_logger()

def get_default_header(token):
    return {
        'Content-Type': 'application/json',
        'Authorization': "Bearer {token}".format(token=token)
    }

class GraphQLHelper:
    def __init__(self, endpoint_url, token, num_of_record_ceiling=5000):
        """
            endpoint_url: Your GraphQL endpoint. 
            token: token for making requests
        """
        self.endpoint = endpoint_url
        self.headers = get_default_header(token)
        self.num_of_record_ceiling = num_of_record_ceiling

    def execute(self, query, variables=None):
        """
            query: the query that we want to execute
            variables: variables in the query if any
        """
        data = {
            'query': query,
            'variables': variables
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
        
            
    def execute_pagination(self, query, schema_names, step_size=50):
        """
        Assuming that we are pulling data has a limit on API calling, 
        we grab all the data by adjusting the value of 'skip',
        the value of 'limit' is set to be step_size.
        query:
            A query with operation name and variables defined.
            To be more specific, 
            we should have two variables: limit and skip.
        schema_names:
            A list of schema names in the returned data for us to count how many records we have. 
            It's also used for merging the records that we have.
        step_size:
            The value for the variable skip. 
            Ideally, we want to adjust it based on some heuristics.
        """
        result_dict = dict()
        num_of_record_read = 0
        request_count = 0
        lead_schema = schema_names[0]
        while num_of_record_read <= self.num_of_record_ceiling:
            variables = {
                "limit": step_size,
                "skip": num_of_record_read
            }
            request_count += 1
            json_data = self.execute(query=query, 
                                     variables=variables)
            if json_data is None:
                LOGGER.error("Error when pulling data from the API endpoint. Please check logs.")
                raise ValueError("Failed to pull data from the API endpoint.")
            else:
                if "data" not in json_data:
                    raise ValueError("Error while getting data. Field 'data' not included in JSON response")
                batch_data = json_data["data"]
                batch_size = len(batch_data[lead_schema])
                if not batch_size:
                    LOGGER.info("No more data comes in. Stop the loop now.") 
                    break
                for schema in schema_names:
                    if schema not in result_dict:
                        result_dict[schema] = batch_data[schema].copy()
                    else:
                        result_dict[schema].extend(batch_data[schema])
                num_of_record_read += batch_size
                if batch_size < step_size:
                    LOGGER.info("We have retrieved all the data. Stop the loop now.")
                    break
                if request_count >= FIREFLIES_LIMIT_PER_MINUTE:
                    request_count = 0
                    LOGGER.info("Exceeded Fireflies' API limit. Wait for a minute.")
                    time.sleep(60)
        return result_dict, num_of_record_read

    def execute_pagination_with_cutoff(self, query, schema_names, cutoff_schema, cutoff_field, cutoff_value, step_size=50):
        """
        An extension of execute_pagination
        Assume that we also have a cutoff value for a cutoff field in a cutoff schema
        for instance, created date is a cutoff field in the cutoff schema transcripts. 
        We stop calling the API once we exceed a timestamp. 
        """
        result_dict = dict()
        num_of_record_read = 0
        request_count = 0
        lead_schema = schema_names[0]
        while num_of_record_read <= self.num_of_record_ceiling:
            variables = {
                "limit": step_size,
                "skip": num_of_record_read
            }
            request_count += 1
            json_data = self.execute(query=query, 
                                     variables=variables)
            if json_data is None:
                LOGGER.error("Error when pulling data from the API endpoint. Please check logs.")
                raise ValueError("Failed to pull data from the API endpoint.")
            else:
                if "data" not in json_data:
                    raise ValueError("Error while getting data. Field 'data' not included in JSON response")
                batch_data = json_data["data"]
                batch_size = len(batch_data[lead_schema])
                if not batch_size:
                    LOGGER.info("No more data comes in. Stop the loop now.") 
                    break
                # Determine when to stop
                cutoff_schema_data = batch_data[cutoff_schema]
                cutoff_index = batch_size
                for index, data in enumerate(cutoff_schema_data):
                    if data[cutoff_field] < cutoff_value:
                        cutoff_index = index
                        LOGGER.info("We determine the cutoff index to be {}".format(cutoff_index))
                        break
                # Update batch_size to the actual one 
                batch_size = cutoff_index
                for schema in schema_names:
                    if schema not in result_dict:
                        result_dict[schema] = batch_data[schema][:cutoff_index].copy()
                    else:
                        result_dict[schema].extend(batch_data[schema][:cutoff_index])
                num_of_record_read += batch_size
                if batch_size < step_size:
                    LOGGER.info("We have retrieved all the data. Stop the loop now.")
                    break
                if request_count >= FIREFLIES_LIMIT_PER_MINUTE:
                    request_count = 0
                    LOGGER.info("Exceeded Fireflies' API limit. Wait for a minute.")
                    time.sleep(60)
        return result_dict, num_of_record_read


# A minimal version of https://github.com/vibeus/tap-shopify-customized/blob/master/tap_shopify/context.py
# We use it to store multiple states
class Context:
    state = {}