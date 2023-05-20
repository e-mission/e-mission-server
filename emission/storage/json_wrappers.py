import bson.json_util as bju
import bson.binary as bbin

# Create wrappers to load and save with the legacy UUID representation
# these wrappers are designed to be drop-in replacements for the existing `bson.json_util.default`
# and `bson.json_util.object_hook`

wrapped_object_hook = lambda s: bju.object_hook(s,
    json_options = bju.RELAXED_JSON_OPTIONS.with_options(
        uuid_representation=bbin.UuidRepresentation.PYTHON_LEGACY))

wrapped_default = lambda o: bju.default(o, json_options = bju.LEGACY_JSON_OPTIONS)

# TODO: Why are the wrapped_default and wrapped_dumps different
# need to see whether the UUID representation really does need to be specified
# and unify them
wrapped_dumps = lambda o: bju.dumps(o, json_options = bju.LEGACY_JSON_OPTIONS.with_options(
    uuid_representation= bbin.UuidRepresentation.PYTHON_LEGACY))

# This doesn't currently seem to require any wrapping, but let's abstract it
# out anyway to avoid hacky changes by interns later
wrapped_loads = lambda s: bju.loads(s)
