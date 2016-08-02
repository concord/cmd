import os

class ContextDirMgr:
    """ Use with a 'with' keyword. All code in context will be in desired
    directory context"""
    def __init__(self, path):
        self.new_dir = os.path.dirname(os.path.abspath(path))

    def __enter__(self):
        self.old_dir = os.getcwd()
        os.chdir(self.new_dir)

    def __exit__(self, value, type, traceback):
        os.chdir(self.old_dir)

def flatten(xs):
    """ Flattens a nested list one level deep"""
    return reduce(lambda m, x: m + x, xs,[])

def flat_map(predicate, xs):
    """ Applys a transformation that transforms list into list(list), then
    flattens the results """
    return reduce(lambda memo, x: memo + x, map(predicate, xs), [])

def find_first_of(predicate, xs):
    """Returns first match of predicate from xs"""
    for x in xs:
        if predicate(x):
            return x
    return None

def split_predicate(predicate, xs):
    """ Returns a tuple pair of lists where items from xs that match
    the predicate fall into the first list and the rest in the second."""
    def splitter(memo, x):
        first, second = memo
        if predicate(x):
            first.append(x)
        else:
            second.append(x)
        return (first, second)

    return reduce(splitter, xs, ([],[]))

def pairs_todict(kvpair_list):
    """ Transform list -> dict i.e. [a=b, c=d] -> {'a':'b', 'c':'d'}"""
    return None if kvpair_list is None else \
    { k:v for k, v in map(lambda c: c.split('='), kvpair_list) }

