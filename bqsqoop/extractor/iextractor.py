from abc import ABC, abstractmethod


class Extractor(ABC):
    """Extractor interface for source table extraction

    Implement the abstract methods `validate_config` and
    `execute` to implement a new type of Extractor.

    Args:
        _config: Extractor sub-config of the Sqoop Job
    """

    def __init__(self, _config):
        self._config = _config
        super().__init__()

    @abstractmethod
    def validate_config(self):
        """implement your config validator here

        Returns: (bool)
            True if valid, else False
        """
        return False    # pragma: no cover

    @abstractmethod
    def extract_to_parquet(self):
        """Extracts data from source to parquet files

        Returns:
            List of all extracted full file path.
        """
        return []   # pragma: no cover
