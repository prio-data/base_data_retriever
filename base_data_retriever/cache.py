import os
import abc
import pickle
import settings
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

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

class BlobStorageCache(AbstractCache):
    def __init__(self,connection_string,container_name):

        self.client = (BlobServiceClient
                .from_connection_string(connection_string)
                .get_container_client(container_name)
            )

    def get(self,k):
        try:
            blob = (self.client
                    .get_blob_client(k)
                    .download_blob()
                )
        except ResourceNotFoundError as rnf:
            raise KeyError(k) from rnf
        else:
            return pickle.loads(blob.content_as_bytes())

    def set(self,k,obj):
        (self.client
            .get_blob_client(k)
            .upload_blob(pickle.dumps(obj))
            )

if settings.PROD:
    cache = BlobStorageCache(
            settings.BLOB_STORAGE_CONNECTION_STRING,
            settings.BLOB_STORAGE_GEN_CACHE_CONTAINER
        )
else:
    cache = FsCache(settings.CACHE_DIR)
