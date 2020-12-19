# tap-youtube-analytics

**Python Version:** `3.7.4`

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the following Youtube APIs:
  - [Youtube Reporting API](https://developers.google.com/youtube/reporting/v1/reports)
  - [Youtube Data API](https://developers.google.com/youtube/v3/docs)
- Youtube Reporting API resources
  - [jobs](https://developers.google.com/youtube/reporting/v1/reference/rest/v1/jobs/list)
  - [reports](https://developers.google.com/youtube/reporting/v1/reference/rest/v1/jobs.reports/list)
  - [report_types](https://developers.google.com/youtube/reporting/v1/reference/rest/v1/reportTypes/list)
  - To generate **Reports**:
    - [Channel Reports](https://developers.google.com/youtube/reporting/v1/reports/channel_reports)
    - [Content Owner Reports](https://developers.google.com/youtube/reporting/v1/reports/content_owner_reports)
- Youtube Data API resources
  - [channels](https://developers.google.com/youtube/v3/docs/channels/list)
  - [playlists](https://developers.google.com/youtube/v3/docs/playlists/list)
  - [playlist_items](https://developers.google.com/youtube/v3/docs/playlistItems/list)
  - [videos](https://developers.google.com/youtube/v3/docs/videos/list)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Authentication
The [**Youtube Analytics Authentication**](https://docs.google.com/document/d/1FEYRL1U1nPZCHoiexIJfHn7RVz_dfx10b4StJVnUoJw) Google Doc provides instructions show how to configure an API app, generate an API key (client_id, client_secret), authenticate and generate a refresh_token, and prepare your tap config.json with the necessary parameters.

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-youtube-analytics
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install target-json
    > pip install target-stitch
    > pip install singer-tools
    > pip install singer-python
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. Include the client_id, client_secret, refresh_token, channel_ids (unique channel IDs in a comma delimited list), start_date (UTC format), and user_agent (tap name with the api user email address).

    ```json
    {
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "refresh_token": "YOUR_REFRESH_TOKEN",
        "channel_ids": "CHANNEL_ID_1, CHANNEL_ID_2, CHANNEL_ID_3ß",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-youtube-analytics <api_user_email@example.com>"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.
    Only the `performance_reports` uses a bookmark. The date-time bookmark is stored in a nested structure based on the endpoint, site, and sub_type.

    ```json
    {
      "currently_syncing": "sitemaps",
      "bookmarks": {
        "playlist_items": "2020-04-08T00:00:00.000000Z",
        "videos": "2020-04-08T00:00:00.000000Z",
        "channel_basic": "2020-04-08T00:00:00.000000Z",
        "channel_province": "2020-04-08T00:00:00.000000Z",
        "channel_traffic_source": "2020-04-08T00:00:00.000000Z"
      }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-youtube-analytics --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

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
    
    While developing the Google Search Console tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_youtube_analytics -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.87/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-youtube-analytics --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
      TBD
    ```
---

Copyright &copy; 2019 Stitch
