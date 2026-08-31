***************
Release History
***************

v0.11.10 (2026-08-24)
====================
What's Changed
..............
* ENH: Add ``--version``/``-V`` argument to ``sync-experiment`` CLI by `@mrakitin <https://github.com/mrakitin>`_ in https://github.com/NSLS2/nslsii/pull/256
* FIX: Use the correct way of specifying numpy dtypes by `@tacaswell <https://github.com/tacaswell>`_ in https://github.com/NSLS2/nslsii/pull/254
* Added function that searches for proposals by PI name by `@jmaruland <https://github.com/jmaruland>`_ in https://github.com/NSLS2/nslsii/pull/249
* Update packaging to pyproject.toml + hatchling; Fix ophyd-async errors in tests by `@thopkins32 <https://github.com/thopkins32>`_ in https://github.com/NSLS2/nslsii/pull/263

**New Contributors**

* `@jmaruland <https://github.com/jmaruland>`_ made their first contribution in https://github.com/NSLS2/nslsii/pull/249

**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.11.9...HEAD


v0.11.9 (2026-05-04)
====================
What's Changed
..............
* Thread through a tla suffix by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/246
* Remove Redis backed RE metadata dict, superseded by external package ``redis_json_dict`` by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/248
* Greatly simplify the ophyd-async providers by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/247
* DOC: update urls due to moving repo by `@tacaswell <https://github.com/tacaswell>`_ in https://github.com/NSLS2/nslsii/pull/251
* Disambiguate dual use of Redis prefix; Add ``redis_db`` option by `@thopkins32 <https://github.com/thopkins32>`_ in https://github.com/NSLS2/nslsii/pull/253

**Breaking Changes**

* ``sync_experiment()`` function argument changed from ``prefix`` to ``endstation`` in https://github.com/NSLS2/nslsii/pull/253 (CLI version is unchanged and always used ``--endstation``)

**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.11.8...v0.11.9


v0.11.8 (2026-02-11)
====================
What's Changed
..............
* Fix issues with ``sync-experiment`` where the beamline name needed to be normalized (Redis) and denormalized (PASS) in different places for beamlines with multiple endstations (SST1, SST2, OPLS) by `@Kezzsim <https://github.com/Kezzsim>`_ in https://github.com/NSLS2/nslsii/pull/244

**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.11.7...v0.11.8


v0.11.7 (2026-01-30)
====================
What's Changed
..............
* Added Redis connection info on verbose by `@RobertSchaffer1 <https://github.com/RobertSchaffer1>`_ in https://github.com/NSLS-II/nslsii/pull/240
* Fixes bug at opls by `@RobertSchaffer1 <https://github.com/RobertSchaffer1>`_ in https://github.com/NSLS-II/nslsii/pull/241
* Add bitnami kafka legacy to fix CI by `@jennmald <https://github.com/jennmald>`_ in https://github.com/NSLS-II/nslsii/pull/242

**Full Changelog**: https://github.com/NSLS-II/nslsii/compare/v0.11.6...v0.11.7


v0.11.6 (2026-01-26)
====================
What's Changed
..............
* Handling passwords through file, and location to new redis locations by `@RobertSchaffer1 <https://github.com/RobertSchaffer1>`_ in https://github.com/NSLS-II/nslsii/pull/239

**Full Changelog**: https://github.com/NSLS-II/nslsii/compare/v0.11.5...v0.11.6


v0.11.5 (2026-01-13)
====================
What's Changed
..............
* FIX: Ensure ``tiled_access_tags`` is list by `@genematx <https://github.com/genematx>`_ in https://github.com/NSLS-II/nslsii/pull/232
* Update Redis by `@thopkins32 <https://github.com/thopkins32>`_ in https://github.com/NSLS-II/nslsii/pull/236
* Configure Redis without SSL by default; Better selection for SSL and password usage by `@thopkins32 <https://github.com/thopkins32>`_ in https://github.com/NSLS-II/nslsii/pull/237
* Fix ``verbose`` mode in ``sync_experiment`` by `@Kezzsim <https://github.com/Kezzsim>`_ in https://github.com/NSLS-II/nslsii/pull/238

**New Contributors**

* `@thopkins32 <https://github.com/thopkins32>`_ made their first contribution in https://github.com/NSLS-II/nslsii/pull/236

**Full Changelog**: https://github.com/NSLS-II/nslsii/compare/v0.11.4...v0.11.5


v0.11.4 (2025-09-04)
====================
What's Changed
..............
* Replace dispatch method with ``generate_datum`` by `@RobertSchaffer1 <https://github.com/RobertSchaffer1>`_ in https://github.com/NSLS2/nslsii/pull/229
* MNT: remove OSX files by `@tacaswell <https://github.com/tacaswell>`_ in https://github.com/NSLS2/nslsii/pull/230

**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.11.2...v0.11.4


v0.11.2 (2025-05-27)
====================
What's Changed
..............
* Add support for the new Tiled Data Access controls to Sync Experiment by `@Kezzsim <https://github.com/Kezzsim>`_ in https://github.com/NSLS-II/nslsii/pull/225
* Always update the cycle information in Redis when running Sync Experiment by `@Kezzsim <https://github.com/Kezzsim>`_ in https://github.com/NSLS-II/nslsii/pull/225
* Add RBD9103 to Ophyd Async devices by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS-II/nslsii/pull/224
* Update pre-commit configuration by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS-II/nslsii/pull/221
* Add helpful RunEngine debug subscriptions by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS-II/nslsii/pull/218
* Remove broken import in conftest by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS-II/nslsii/pull/217

**Full Changelog**: https://github.com/NSLS-II/nslsii/compare/v0.11.1...v0.11.2


v0.11.1 (2025-03-27)
====================
What's Changed
..............
* Move redis parameters to the kwarg-only section by `@jennmald <https://github.com/jennmald>`_ in https://github.com/NSLS2/nslsii/pull/214


**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.11.0...v0.11.1


v0.11.0 (2025-03-25)
====================
What's Changed
..............
* Fix: default broker to None by `@maffettone <https://github.com/maffettone>`_ in https://github.com/NSLS2/nslsii/pull/205
* Refactor auth mechanism in sync-experiment by `@genematx <https://github.com/genematx>`_ in https://github.com/NSLS2/nslsii/pull/198
* Fix: configure_kafka_publisher assumed a string by `@maffettone <https://github.com/maffettone>`_ in https://github.com/NSLS2/nslsii/pull/209
* Remove persistent dict and add redis_json_dict support by `@jennmald <https://github.com/jennmald>`_ in https://github.com/NSLS2/nslsii/pull/212
* Adds if_touch_beamline function to common by `@jennmald <https://github.com/jennmald>`_ in https://github.com/NSLS2/nslsii/pull/211

**New Contributors**

* `@jennmald <https://github.com/jennmald>`_ made their first contribution in https://github.com/NSLS2/nslsii/pull/212

**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.10.7...v0.11.0

v0.10.7 (2024-10-30)
====================
What's Changed
..............
* CI: only use the `published` event for PyPI releases by `@mrakitin <https://github.com/mrakitin>`_ in https://github.com/NSLS2/nslsii/pull/203
* Remove 'finally' that is eating exceptions by `@nmaytan <https://github.com/nmaytan>`_ in https://github.com/NSLS2/nslsii/pull/200
* Use a configuration file from `n2sn_user_tools` for `sync-experiment` by `@mrakitin <https://github.com/mrakitin>`_ in https://github.com/NSLS2/nslsii/pull/202
* Deprecate the webcam class by `@mrakitin <https://github.com/mrakitin>`_ in https://github.com/NSLS2/nslsii/pull/204


**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.10.6...v0.10.7

v0.10.6 (2024-10-29)
====================
What's Changed
..............
* Adding pre-commit config setup from ophyd async by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/191
* Update PyPI when release is created or published by `@padraic-shafer <https://github.com/padraic-shafer>`_ in https://github.com/NSLS2/nslsii/pull/197
* Adding standard ophyd async path and filename providers by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/192
* Update authentication method for sync experiment to be more robust in… by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/199

## New Contributors
* `@padraic-shafer <https://github.com/padraic-shafer>`_ made their first contribution in https://github.com/NSLS2/nslsii/pull/197

**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.10.5...v0.10.6

v0.10.5 (2024-09-27)
====================
What's Changed
..............
* Move srx caproto iocs by `@jwlodek <https://github.com/jwlodek>`_ in https://github.com/NSLS2/nslsii/pull/195
* Make sync-experiment work for commissioning proposals by `@nmaytan <https://github.com/nmaytan>`_ in https://github.com/NSLS2/nslsii/pull/196


**Full Changelog**: https://github.com/NSLS2/nslsii/compare/v0.10.4...v0.10.5

v0.10.4 (2024-09-18)
====================
* Add SRX MAIA code
* Remove distutils
* Fix sync_experiment for SST 
* Fix docker compose usage

v0.10.3 (2024-06-28)
====================
* Add additional property 'type' to sync-experiment, requested by SRX.
* Add pmac kill device signal to delta tau motor controls.

v0.10.2 (2024-05-31)
====================
* Add a CLI tool to get IOC hostname for a given PV
* Add more proposal info to sync-experiment tool
* Support running nslsii.start_experiment as CLI

v0.10.1 (2024-05-30)
====================
* rename sync-redis to sync-experiment

v0.10.0 (2024-05-29)
====================
* add a utility to start/switch beamline experiment

v0.9.1 (2023-06-08)
====================
* add optional call_returns_result parameter to be propagated to the RunEngine
* add an ophyd class for a webcam streaming to a URL (Axis cameras)
* update data handling for flyscaning with Xspress3

v0.9.0 (2023-01-20)
===================
* fix incorrect usage of ``prefix=`` keyword argument in tests 
* add ``nslsii.areadetector.xspress3.Xspress3ExternalFileReference.dtype_str``

v0.8.0 (2022-12-19)
===================
* add ophyd classes for QEPro spectrometer IOC
* rationalize global key names for ``nslsii.md_dict.RunEngineRedisDict``
* add time series and units PVs to ``nslsii.areadetector.xspress3``
* add external file reference class to ``nslsii.areadetector.xspress3``
* add hdf5 plugin class to ``nslsii.areadetector.xspress3``

v0.7.0 (2022-08-05)
===================
* support for new sections in the Kafka configuration file
* simplified Kafka docker-compose script

v0.6.0 (2022-07-22)
===================
* improvements to ``nslsii.md_dict.RunEngineRedisDict``

v0.5.0 (2022-06-28)
===================
* add ``nslsii.md_dict.RunEngineRedisDict``

v0.4.0 (2022-04-05)
===================
* simplify ``nslsii.areadetector.xspress3`` component hierarchy (API change)
* replace deprecated IPython ``magic()`` calls with ``run_line_magic()``
* correction to documentation for ``nslsii.configure_base``

v0.3.2 (2022-01-20)
===================
* add a srx resource transform

v0.3.1 (2022-01-13)
===================
* fix a Kafka configuration bug in ``nslsii.configure_base``

v0.3.0 (2021-12-20)
===================
* add Kafka configuration parameters and support to ``nslsii.configure_base``

v0.2.2 (2021-12-08)
===================
* add the ``bec_derivative`` kwarg to ``nslsii.configure.base``
* add GitHub Action workflow to publish to PyPI automatically

v0.2.1 (2021-08-27)
===================
* reinstate ``bluesky_kafka`` conditional import with tests
* add GitHub Action for CI

v0.2.0 (2021-08-24)
===================
* updated documentation for beamline RunEngine Kafka topic names
* import ``bluesky_kafka`` only when needed in ``nslsii.configure_base``
* resolved an issue with importing ``nslsii.iocs``
* improved exception handling when bluesky documents are published as Kafka messages
* send beamline log output to syslog
* added ``nslsii.areadetector.xspress3`` to support APS Xspress3 IOC

v0.1.3 (2021-03-29)
===================
* added environment variable for kafka bootstrap servers
* change bluesky kafka topic naming scheme

v0.1.2 (2021-01-26)
===================
* fix the ``TwoButtonShutter`` class to be compatible with ophyd 1.6.0+
* added log propagation configuration to reduce log noise

v0.1.1 (2020-10-26)
===================
* update manifest and license files
* make minimal traceback reporting optional
* changes to allow 'nslsii' to load without IPython
* update the status of the xspress3 detector on unstaging

v0.1.0 (2020-09-04)
===================
* synchronize xspress3 code with hxntools
* new TwoButtonShutter configuration
* change Signal.value to Signal.get()
* handle Kafka exceptions

v0.0.17 (2020-08-06)
====================
* update the function that subscribes a Kafka producer to the RunEngine

v0.0.16 (2020-06-26)
====================
* create the default logging directory if it does not exist

v0.0.15 (2020-06-16)
====================
* use appdirs to determine default logging directory
* add a function to subscribe a Kafka producer to the RunEngine

v0.0.10 (2019-06-06)
====================

Features
--------
* Add EPSTwoStateIOC class for simulation
