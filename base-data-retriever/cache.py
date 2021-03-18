import os
import abc
import pickle
import settings

class AbstractCache(abc.ABC):
    @abc.abstractmethod
    def get(self,k):
        pass
    @abc.abstractmethod
    def set(self,k,obj):
        pass

class FsCache(AbstractCache):
    def __init__(self,base_path):
        self.base_path = base_path
        try:
            os.makedirs(self.base_path)
        except FileExistsError:
            pass

    def get(self,k):
        try:
            with open(os.path.join(self.base_path,k),"rb") as f:
                return pickle.load(f)
        except FileNotFoundError as fne:
            raise KeyError from fne

    def set(self,k,obj):
        with open(self.resolve(k),"wb") as f:
            pickle.dump(obj,f)

    def resolve(self,k):
        return os.path.join(self.base_path,k)

if settings.PROD:
    cache = FsCache(settings.CACHE_DIR)
else:
    cache = FsCache(settings.CACHE_DIR)
