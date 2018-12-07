def stash_it(label, persist=True):

    def outer(f):
        def inner(uids, *args, **kwargs):
            result = f(uids, *args, **kwargs)
            doc = {'raw_run_starts': uids,
                   'function': f.__qualname__,
                   'args': args,
                   'kwargs': kwrags}
            if persist:
                # Glossing over something big here.... assets?
                doc['result'] = result
            getattr(db, label).insert(doc)
            return result
        return inner
    return outer


# Write the parameters and, if persist=True, also the data so it does not need
# to be recomputed.

@stash_it('step1', persist=False)
def my_analysis_function(uids, param1, param2):
    headers = [db[uid] for uid in uids]
    # The job of this is to unpack the arrays and metadata that it needs from
    # headers and pass that into numpy/scipy/rixs functions.
    return rixs(...)


# Access saved data *or* recompute. User can't tell the difference.
db.step1[uid]
