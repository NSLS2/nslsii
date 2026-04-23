import os
from redis import Redis
import socket

redis_hosts = [
    "xf02id1-six-redis1.nsls2.bnl.gov",
    "xf03id1-hxn-redis1.nsls2.bnl.gov",
    "xf04bm-maia-redis1.nsls2.bnl.gov",
    "xf04bm-xfm-redis1.nsls2.bnl.gov",
    "xf04id1-isr-redis1.nsls2.bnl.gov",
    "xf05id2-srx-redis1.nsls2.bnl.gov",
    "xf06bm-bmm-redis1.nsls2.bnl.gov",
    "xf07bm-qas-redis1.nsls2.bnl.gov",
    "xf07id1-haxpes-redis1.nsls2.bnl.gov",
    "xf07id1-nexafs-redis1.nsls2.bnl.gov",
    "xf07id1-rsoxs-redis1.nsls2.bnl.gov",
    "xf07id1-ucal-redis1.nsls2.bnl.gov",
    "xf08bm-tes-redis1.nsls2.bnl.gov",
    "xf08id1-iss-redis1.nsls2.bnl.gov",
    "xf09id1-cdi-redis1.nsls2.bnl.gov",
    "xf10id1-ixs-redis1.nsls2.bnl.gov",
    "xf11id1-chx-redis1.nsls2.bnl.gov",
    "xf11bm-cms-redis1.nsls2.bnl.gov",
    "xf12id1-opls-redis1.nsls2.bnl.gov",
    "xf12id2-smi-redis1.nsls2.bnl.gov",
    "xf16id1-lix-redis1.nsls2.bnl.gov",
    "xf17bm-xfp-redis1.nsls2.bnl.gov",
    "xf17id1-amx-redis1.nsls2.bnl.gov",
    "xf17id2-fmx-redis1.nsls2.bnl.gov",
    "xf18id1-fxi-redis1.nsls2.bnl.gov",
    "xf19id2-nyx-redis1.nsls2.bnl.gov",
    "xf21id1-arpes-redis1.nsls2.bnl.gov",
    "xf21id1-xpeem-redis1.nsls2.bnl.gov",
    "xf23id1-csx-redis1.nsls2.bnl.gov",
    "xf23id2-ios-redis1.nsls2.bnl.gov",
    "xf27id1-hex-redis1.nsls2.bnl.gov",
    "xf28id1-pdf-redis1.nsls2.bnl.gov",
    "xf28id2-xpd-redis1.nsls2.bnl.gov",
    "xf28id2-xpdd-redis1.nsls2.bnl.gov",
    "xf31id1-tst-redis1.nsls2.bnl.gov",
]


def open_redis_client(
    redis_url=None,
    redis_port=None,
    redis_ssl=False,
    redis_db: int = 0,
) -> Redis:
    """
    Helper function to get the redis client connection.
    """
    if os.getenv("REDIS_HOST"):
        redis_url = os.getenv("REDIS_HOST")
    if redis_url is None:
        if redis_ssl:
            client_loc_id = socket.gethostname().split("-")[0]
            client_locations = [
                location for location in redis_hosts if client_loc_id in location
            ]
            if len(client_locations) != 1:
                raise RuntimeError(
                    "Failed to derive redis server url, please specify using the "
                    "redis_url argument."
                )
            else:
                redis_url = client_locations[0]
        else:
            tla = os.getenv("BEAMLINE_ACRONYM").lower()
            redis_url = f"info.{tla}.nsls2.bnl.gov"

    if redis_ssl:
        redis_pw = os.getenv("REDIS_PASSWORD")
        if not redis_pw:
            redis_secret_file = os.getenv(
                "REDIS_SECRET_FILE", "/etc/bluesky/redis.secret"
            )
            with open(redis_secret_file, "r", encoding="utf-8") as password_file:
                redis_pw = password_file.read().strip()
    else:
        redis_pw = None

    redis_port = os.getenv("REDIS_PORT", redis_port)
    if not redis_port:
        if not redis_ssl:
            redis_port = 6379
        else:
            redis_port = 6380

    conn = Redis(
        host=redis_url,
        port=redis_port,
        ssl=redis_ssl,
        password=redis_pw,
        db=redis_db,
    )
    return conn
