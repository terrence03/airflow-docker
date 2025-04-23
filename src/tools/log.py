import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import pendulum


class Log:
    def __init__(self, log_file, level=logging.INFO):
        self.log_file = Path(log_file)
        self.level = level
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger_name = f"log_{self.log_file.stem}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(self.level)
        logger.propagate = False  # 防止傳遞到 root logger

        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s - %(message)s")

            # 設定時區為 Asia/Taipei
            class PendulumTaipei:
                @staticmethod
                def converter(*args):
                    return pendulum.now("Asia/Taipei").timetuple()

            formatter.converter = PendulumTaipei.converter

            # 使用 TimedRotatingFileHandler 每天產生新 log，最多保留 7 天
            file_handler = TimedRotatingFileHandler(
                filename=self.log_file, when="midnight", backupCount=7, encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def write_update_info_to_logger(self, update_status: str, update_date: str = None):
        if update_date:
            update_info = f"{update_status} - {update_date}"
        else:
            update_info = update_status
        self.logger.info(update_info)

    def get_last_update_date(self):
        try:
            if not self.log_file.exists():
                return ""
            with self.log_file.open("r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    parts = line.strip().split(" - ")
                    if len(parts) >= 3:
                        return parts[-1]  # update_date 是最後一段
                return ""
        except Exception:
            return ""
