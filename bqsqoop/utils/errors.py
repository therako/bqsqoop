
class MissingConfigError(ValueError):
    def __init__(self, config_name, root_name):
        self.error = 'Missing config for {0} under {1}'.format(
            config_name, root_name)

    def __str__(self):
        return self.error
