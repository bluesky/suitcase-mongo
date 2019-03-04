# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

from bluesky import RunEngine
from bluesky.plans import scan
from mongobox import MongoBox
from ophyd.sim import det, motor

# From event_model import NumpyEncoder
from suitcase.mongo_embedded import Serializer

volatile_box = MongoBox()
permanent_box = MongoBox()

volatile_box.start()
permanent_box.start()

volatile_db = volatile_box.client().db
permanent_db = permanent_box.client().db

serializer = Serializer(volatile_db, permanent_db)

RE = RunEngine({})
RE.subscribe(serializer)
RE(scan([det], motor, 1, 10, 10000))
