class AuthenticationError(Exception):

    def __init__(self, message=None):
        self.message = message


class ResourceNotFound(Exception):

    def __init__(self, message=None):
        self.message = message


class EmailExistsError(Exception):
    def __init__(self, message=None):
        self.message = message