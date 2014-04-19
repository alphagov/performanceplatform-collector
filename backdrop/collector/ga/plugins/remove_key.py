
class RemoveKey(object):

    """
    Remove all of the specified keys from the input documents.
    """

    def __init__(self, *remove_keys):
        self.remove_keys = remove_keys

    def __call__(self, documents):
        for document in documents:
            for key in self.remove_keys:
                del document[key]
        return documents
