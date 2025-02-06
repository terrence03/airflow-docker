import logging


class Log:
    def __init__(self, log_file):
        self.log_file = log_file
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """Set up the logger"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def write_update_info_to_logger(self, update_date: str, update_status: str):
        """Test the logger"""
        update_info = f"{update_date} - {update_status}"
        self.logger.info(update_info)

    def get_last_update_date(self):
        """Get the last update date"""
        with self.log_file.open() as _:
            if not _.readlines():
                return ""
            _.seek(0)
            last_line = _.readlines()[-1]
            last_update_date = last_line.split(" - ")[1]
            return str(last_update_date)
