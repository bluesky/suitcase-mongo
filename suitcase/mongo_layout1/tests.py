# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.


from . import Serializer
serializer = suitcase.mongo_layout1.Serializer('mongodb://localhost:27017/metadatastore', 'mongodb://localhost:27017/asset_registry')
RE.subscribe(serializer)
from bluesky.plans import scan
from ophyd.sim import det, motor
RE(scan([det], motor, 1, 3, 3))
