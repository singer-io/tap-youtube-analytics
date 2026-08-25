# Changelog

## 0.2.0
  * Streams the credentials cannot access are now excluded from the catalog during discovery instead of failing the entire discover run. [#16](https://github.com/singer-io/tap-youtube-analytics/pull/16)
  * Refactored discovery to a centralized access-check flow and added/updated unit tests for access checks and stream pruning.

## 0.1.0
  * Tap refactoring
  * Fixes dependabot issues
  * Updated dependencies to latest versions: requests==2.33.1, singer-python==6.8.0, pylint==4.0.5
  * Adds `parent-tap-stream-id` as discoverable metadata


## 0.0.1
  * Initial commit
