# tap-youtube-analytics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)

This tap:

- Pulls raw data from the [youtube-analytics API].
- Extracts the following resources:
    - [Channels](https://developers.google.com/youtube/v3/docs/channels/list)

    - [Playlists](https://developers.google.com/youtube/v3/docs/playlists/list)

    - [PlaylistItems](https://developers.google.com/youtube/v3/docs/playlistItems/list)

    - [Videos](https://developers.google.com/youtube/v3/docs/videos/list)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


[channels](https://developers.google.com/youtube/v3/docs/channels/list)
- Primary keys: ["id"]
- Replication strategy: FULL_TABLE

[playlists](https://developers.google.com/youtube/v3/docs/playlists/list)
- Primary keys: ["id"]
- Replication strategy: FULL_TABLE

[playlist_items](https://developers.google.com/youtube/v3/docs/playlistItems/list)
- Primary keys: ["id"]
- Replication strategy: INCREMENTAL

[videos](https://developers.google.com/youtube/v3/docs/videos/list)
- Primary keys: ["id"]
- Replication strategy: INCREMENTAL



## Authentication

The tap uses Google's OAuth 2.0 flow. When the connector runs inside Qlik
Cloud, the authorization code exchange is handled automatically and the stored
refresh token is injected into the tap configuration. If you're running the tap
locally, provide `client_id`, `client_secret`, and a valid `refresh_token` in
the config file.

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-youtube-analytics
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-youtube-analytics <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.
   
    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-youtube-analytics <api_user_email@your_company.com>",
        "request_timeout": 300,
        ...
    }
    ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "playlists",
        "bookmarks": {
            "videos": {
                "published_at": "2019-09-27T22:34:39.000000Z"
            }
        }
    }
    ```

    The tap now stores bookmarks in this nested structure. If an existing state file still contains the older flat timestamps (for example `"videos": "2019-09-27T22:34:39.000000Z"`), the tap will automatically migrate that value on the next run, but we recommend updating persisted state files and documentation to the new shape.

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-youtube-analytics --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md)

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md)

    For Sync mode:
    ```bash
    > tap-youtube-analytics --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-youtube-analytics --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-youtube-analytics --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the youtube-analytics tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md)
    ```bash
    > pylint tap_youtube-analytics -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools)
    ```bash
    > tap_youtube-analytics --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .[dev]
    ```
---

Copyright &copy; 2019 Stitch
