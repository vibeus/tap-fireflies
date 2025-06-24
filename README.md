# tap-fireflies

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Fireflies](https://docs.fireflies.ai/examples/overview)
- Extracts the following resources:
  - [Users](https://docs.fireflies.ai/graphql-api/query/users)
  - [Transcripts](https://docs.fireflies.ai/graphql-api/query/transcripts)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

Credit: We build this project based on the structure of [tap-intercom](https://github.com/singer-io/tap-intercom).

## Quick start

1. Install
   First, clone this repo. Then, do 
   ```
   pip install .
   ```
2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:
   ```json
    {
      "access_token": "fireflies_access_token",
      "request_timeout": 300,
      "start_date": "2025-06-10T00:00:00.000Z"
    }
    ```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this). Notice that its format should be `%Y-%m-%dT%H:%M:%S.3fZ`.

   The `access_token` is the token of Fireflies. You can adjust the timeout setting `request_timeout` accordingly.

4. Run the Tap in Discovery Mode
    ```
    tap-fireflies --config config.json --discover > catalog.json 
    ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode
    ```
    tap-fireflies -c config.json --catalog catalog.json > output.txt

---

Copyright &copy; 2025 Vibe, Inc.
