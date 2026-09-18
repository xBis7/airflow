<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [UX demo environment](#ux-demo-environment)
  - [Usage](#usage)
  - [What happens at startup](#what-happens-at-startup)
  - [Data volume](#data-volume)
  - [Dags](#dags)
  - [Exposing it](#exposing-it)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# UX demo environment

A throwaway setup that gives a UX designer an Airflow UI with realistic history to explore,
with the Calendar tab in mind. This branch is not meant to be merged.

## Usage

```bash
breeze start-airflow --backend postgres
```

That is the only command. The UI is on port `28080` (`admin` / `admin`).

## What happens at startup

`scripts/in_container/check_environment.sh` calls `dev/ux_demo/seed.sh` once the database is
migrated and before the scheduler, API server and Dag processor start. The seed step:

1. Copies `dev/ux_demo/dags/` to `/files/dags/ux_demo/` (`files/` is gitignored, so the Dags
   cannot live there on the branch).
2. Serializes only those Dags, so startup time does not depend on other files in `files/dags`.
3. Deletes **all** runs of the `ux_demo_*` Dags and recreates their history relative to the
   current time, with task instances for every run.
4. Sets which Dags are paused.

It runs on every start, so restarting brings the dates forward. A run keeps the same outcome
across re-seeds because outcomes are derived from a hash of the run's identity. A seeding
failure does not block startup; output is kept in `files/ux_demo_seed.log`.

## Data volume

| Profile | Runs | Use |
|---------|------|-----|
| `full` (default) | ~20k | what the designer should see; seeds in about 20 s |
| `small` | ~1.5k | quick local check |

Select the profile with `UX_DEMO_SEED_PROFILE`. Breeze only passes known variables into the
container, so set it in the gitignored `files/airflow-breeze-config/environment_variables.env`:

```bash
mkdir -p files/airflow-breeze-config
echo "UX_DEMO_SEED_PROFILE=small" >> files/airflow-breeze-config/environment_variables.env
```

## Dags

All Dags are tagged `ux-demo`.

| Dag | Schedule | State | Shows |
|-----|----------|-------|-------|
| `ux_demo_hourly_etl` | hourly | active | long history, failure clusters, outage gaps |
| `ux_demo_frequent_sync` | every 10 min | active | several runs per hour: gradient in the hourly view |
| `ux_demo_daily_report` | daily | active | multi-year daily view, multi-day failure streaks |
| `ux_demo_weekday_export` | weekdays | active | weekly rhythm, planned runs skipping weekends |
| `ux_demo_monthly_billing` | monthly | active | very sparse periodic runs |
| `ux_demo_adhoc_reprocess` | none | active | bursts of manual runs, no planned runs; trigger it to watch a live run |
| `ux_demo_state_showcase` | every 15 min | paused | test card: pure/mixed state cells, frozen running and queued runs |
| `ux_demo_new_pipeline` | daily | paused | never ran: empty state |

Active Dags keep running for real (tasks sleep a few seconds and occasionally fail), so recent
runs have real logs. Seeded task instances have no log files.

Paused Dags are paused on purpose: the scheduler ignores their runs, which keeps the seeded
`running`/`queued` runs frozen. The Calendar computes planned runs from the last run forward
without looking at the current time, so a paused Dag shows "planned" cells in the past once the
environment has been up for a while. Restart to re-seed.

## Exposing it

Breeze publishes the web port, the SSH port and the database port on all interfaces, and
Docker-published ports bypass `ufw`. Restrict access with the cloud security group: open only
`28080` to the designer's IP. Credentials are `admin` / `admin` over plain HTTP, so keep the
host free of cloud credentials and do not commit a changed password to a public fork.
