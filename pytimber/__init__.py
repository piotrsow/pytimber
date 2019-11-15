# -*- coding: utf-8 -*-

from .pytimber import LoggingDB
from .dataquery import (DataQuery, parsedate, dumpdate,
                        flattenoverlap, set_xaxis_date,
                        set_xaxis_utctime, set_xlim_date, get_xlim_date)
from .LHCBSRT import BSRT
from .LHCBWS import BWS

from . import timberdata

from .pagestore import PageStore

from .nxcals import NXCals

__version__ = "2.7.3"

__cmmnbuild_deps__ = [
    "accsoft-cals-extr-client",
    "accsoft-cals-extr-domain",
    "pytimber-utils",
#    "lhc-commons-cals-utils",
#    "slf4j-log4j12",
#    "slf4j-api",
#    "log4j",
#    "lhc-nxcals-extraction-utils",
#    "spring-boot-starter",
#    "nxcals-backport-api",
#    "nxcals-extraction-starter",
#    "nxcals-hadoop-pro-config",
]

nxcals=[
    "nxcals-data-access",
    "nxcals-common-spark",
    "nxcals-hadoop-pro-config",
    "spark-core_2.11",
    "spark-yarn_2.11",
    "spark-sql_2.11",
    "spark-hive_2.11",
    "spark-avro_2.11",
    "hadoop-client",
        ]
