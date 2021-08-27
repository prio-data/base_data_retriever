
from toolz.functoolz import compose
from pymonad.either import Left, Right, Either
import numpy as np

make_then = lambda fn: lambda x: x.then(fn)
then_compose = lambda fns, fn: compose(*map(make_then, fns), fn)

def is_monotonic(v: np.array) -> Either[bool, str]:
    """is_monotonic
        Checks whether an np array is a monotonic vector of ints, meaning
        it only contains successive numbers.
    """
    return then_compose([
        lambda v: Right(len(v) + (v[0]-1) == v[-1]),
        lambda v: Right(np.unique(v)),
        lambda v: Right(np.sort(v))],
        lambda v: Right(v) if v.dtype == int else Left(
            f"Expected vector of type int, got vector of {v.dtype}"
            ))(v)
