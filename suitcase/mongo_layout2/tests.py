# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

from event_model import NumpyEncoder
from __init__ import Serializer
from mongobox import MongoBox

volatile_box = MongoBox()
permanent_box = MongoBox()

volatile_box.start()
permanent_box.start()

volatile_db = volatile_box.client().db
permanent_db = volatile_box.client().db

serializer = Serializer(volatile_db, permanent_db)
RE.subscribe(serializer)
from bluesky.plans import scan
from ophyd.sim import det, motor
RE(scan([det], motor, 1, 3, 3))

