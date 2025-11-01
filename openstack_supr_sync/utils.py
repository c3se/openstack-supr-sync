import json
from statistics import mean
from openstack_supr_sync.config import config
from Levenshtein import jaro_winkler

with open(config['bad_word_list']) as file:
    bad_word_list = json.load(file)


def yield_string_slices(to_slice: str, to_compare: str):
    l1 = len(to_slice)
    l2 = len(to_compare)
    if l2 > l1:
        yield to_slice
        return
    test_range = range(l2, l1 + 1)
    slices = []
    for size in test_range:
        slices += [slice(s, size + s) for s in range(l1 - size + 1)]
    for s in slices:
        yield to_slice[s]


def get_profanity_score(to_check: str):
    """Calculates a relatively crude profanity score based on a bad word list
    using Jaro-Winkler similarity. Use a high cutoff, like 0.95."""
    items = []
    for entry in bad_word_list:
        candidates = [jaro_winkler(entry, slice) for slice in yield_string_slices(to_check, entry)]
        candidates = list(reversed(sorted(candidates)))
        items.append(mean(candidates[:3]))
    return max(items)
