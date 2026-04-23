***************
Release History
***************

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
