

class MetadataStoreException(Exception):
    pass


class PathAlreadyExists(MetadataStoreException):
    pass


class MetadataAlreadyExists(MetadataStoreException):
    pass
