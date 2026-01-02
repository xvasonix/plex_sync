# plex_sync

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/26b47c5db63942f28f02f207f692dc85)](https://www.codacy.com/gh/xvasonix/plex_sync/dashboard?utm_source=github.com&utm_medium=referral&utm_content=xvasonix/plex_sync&utm_campaign=Badge_Grade)

Sync watched between plex servers locally

## Description

Keep in sync all your users watched history between plex servers locally. This uses file names and provider ids to find the correct episode/movie between them. This is not perfect but it works for most cases. You can use this for as many servers as you want by entering multiple options in the .env plex section separated by commas.

## Features

### Plex

- [x] Match via filenames
- [x] Match via provider ids
- [x] Map usernames
- [x] Use single login
- [x] One way/multi way sync
- [x] Sync watched
- [x] Sync in progress
- [x] Cron Scheduling

## Configuration

Full list of configuration options can be found in the [.env.sample](.env.sample)

## Installation

### Baremetal

- [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

- Create a .env file similar to .env.sample; fill in baseurls and tokens, **remember to uncomment anything you wish to use** (e.g., user mapping, library mapping, black/whitelist, etc.)

- Run

  ```bash
  uv run main.py
  ```

### Docker

- Build docker image

  ```bash
  docker build -t plex_sync .
  ```

- or use pre-built image

  ```bash
  docker pull xvasonix/plex_sync:latest
  ```

#### With variables

- Run

  ```bash
  docker run --rm -it -e PLEX_TOKEN='SuperSecretToken' xvasonix/plex_sync:latest
  ```

#### With .env

- Create a .env file similar to .env.sample and set the variables to match your setup

- Run

  ```bash
   docker run --rm -it -v "$(pwd)/.env:/app/.env" xvasonix/plex_sync:latest
  ```

## Troubleshooting/Issues



- Configuration
  - Do not use quotes around variables in docker compose
  - If you are not running all supported servers simultaneously, make sure to comment out the server url and token of the server you aren't using.

## Contributing

I am open to receiving pull requests. If you are submitting a pull request, please make sure run it locally for a day or two to make sure it is working as expected and stable.

## License

This is currently under the GNU General Public License v3.0.
