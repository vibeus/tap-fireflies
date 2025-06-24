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

---

Copyright &copy; 2025 Vibe, Inc.
