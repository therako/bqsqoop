from .iextractor import Extractor   # noqa
from .elasticsearch import ElasticSearchExtractor


_extractors = {
    "es": ElasticSearchExtractor,
    "elasticsearch": ElasticSearchExtractor,
}


def get_extractor_for(config):
    """Fetches extractor for given job config.

    :param config: A dict containg sqoop job config.
    returns: An Extractor object
    """
    _extractor_config = config.get("extractor")
    if(len(_extractor_config) != 1):
        raise Exception(
            "Job takes only one extractor, given {}".format(
                _extractor_config))
    _extractor_src, _extractor_config = next(iter(_extractor_config.items()))
    _extractor = _extractors.get(_extractor_src)
    if(_extractor):
        return _extractor(_extractor_config)
    else:
        raise Exception(
            "Unknown extractor given {}".format(
                _extractor_config))
