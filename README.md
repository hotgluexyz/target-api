# target-api

`target-api` is a Singer target for Api.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-api
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-api.git@main
```

-->

## Configuration

### Accepted Config Options

#### Capabilities

* `about`
* `hotglue-exceptions-classes`

#### Settings

| Setting          | Required | Default | Description |
|:-----------------|:--------:|:-------:|:------------|
| url              | True     | None    | API endpoint URL template. Supports placeholders: {stream}, {tenant}, {tenant_id}, {flow}, {flow_id}, {tap}, {connector_id}. |
| method           | False    | POST    | HTTP method to use when sending records to the API. |
| auth             | False    | None    | Enable API key authentication via the configured header. |
| api_key          | False    | None    | API key value sent in the authentication header. |
| api_key_header   | False    | x-api-key | HTTP header name used for API key authentication. |
| api_key_url      | False    | None    | Append the API key as a query parameter on the request URL. |
| user_agent       | False    | target-api <hello@hotglue.xyz> | User-Agent header value for API requests. |
| custom_headers   | False    | None    | Additional HTTP headers to include on each request. |
| timeout          | False    |     600 | Request timeout in seconds. |
| process_as_batch | False    | None    | Send records in batches instead of one record per request. |
| batch_size       | False    |     100 | Maximum number of records per batch request. |
| max_size_in_bytes| False    | None    | Maximum approximate batch payload size in bytes before flushing. |
| inject_batch_ids | False    | None    | Add a unique hgBatchId field to each record in a batch. |
| add_stream_key   | False    | None    | Add the stream name as a stream field on each record. |
| metadata         | False    | None    | Metadata object (JSON string or object) merged into each record. |
| enforce_order    | False    | None    | Process sinks sequentially to preserve record order. |
| post_empty_record| False    | None    | Post an empty record when a stream has a schema but no records. |

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-api --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-api` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-api --version
target-api --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-api --config /path/to/target-api-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-api` CLI interface directly using `poetry run`:

```bash
poetry run target-api --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-api
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-api --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-api
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
