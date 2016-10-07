try:
    import cPickle as pickle
except:
    import pickle

serialize = pickle.dumps
deserialize = pickle.loads
