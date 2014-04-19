class Comment(object):

    """
    Plugin which ignores all of its parameters. Useful for putting comments in
    the list of plugins.
    """

    def __init__(self, *args):
        pass

    def __call__(self, documents):
        return documents
