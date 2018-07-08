import tqdm


class ProgressBar():
    def __init__(self, total_length, position=0, enabled=True):
        """Helper for a progress bar in cli

        Args:
            total_length (int): 100% length of the progress bar
            position (int): Initial postition to start the progress bar at
                Default: 0
            enabled (bool): To make a calls to helper but not actually show
                a progress bar. Default: True
        """
        self._enabled = enabled
        if self._enabled:
            self._progress_bar = tqdm.tqdm(
                total=total_length, position=position,
                desc="slice-%s" % position, ascii=True)

    def move_progress(self, length):
        """Move the progress bar to n position

        Args:
            length (int): N positions to move w.r.t total_length
        """
        if self._enabled:
            self._progress_bar.update(length)
