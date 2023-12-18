import datafusion
import pyarrow.compute as pc
import pyarrow as pa


def is_even(array: pa.Array) -> pa.Array:
    return pc.equal(pc.bit_wise_and(array, 1), 0).fill_null(False)


is_even_arr = datafusion.udf(
    is_even,
    [pa.int32()],
    pa.bool_(),
    "stable",
    name="is_even",
)


def register(ctx):
    ctx.register_udf(is_even_arr)
