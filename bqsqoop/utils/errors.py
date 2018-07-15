
class MissingConfigError(ValueError):
    def __init__(self, config_name, root_name):
        self.error = 'Missing config for {0} under {1}'.format(
            config_name, root_name)

    def __str__(self):
        return self.error


class MissingDataError(ValueError):
    def __init__(self):
        self.error = 'Missing data'

    def __str__(self):
        return self.error


class InvalidTypeError(ValueError):
    def __init__(self, data_type):
        self.error = "Wrong type, it's supposed to be a {0}".format(data_type)

    def __str__(self):
        return self.error
