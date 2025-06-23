import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import pendulum


class TaipeiFormatter(logging.Formatter):
    """自訂 Formatter,將 asctime 設為 Asia/Taipei 時區"""

    def converter(self, *args):
        return pendulum.now("Asia/Taipei").timetuple()


class Log:
    """
    日誌工具,支援每日輪替、時區、查詢最後更新日期.
    """

    _loggers = {}

    def __init__(self, log_file: str, level: int = logging.INFO):
        self.log_file = Path(log_file)
        self.level = level
        self.logger = self._get_or_create_logger()

    def _get_or_create_logger(self) -> logging.Logger:
        logger_name = f"log_{self.log_file.resolve()}"
        if logger_name in Log._loggers:
            return Log._loggers[logger_name]

        logger = logging.getLogger(logger_name)
        logger.setLevel(self.level)
        logger.propagate = False

        formatter = TaipeiFormatter("%(asctime)s - %(message)s")

        file_handler = TimedRotatingFileHandler(
            filename=self.log_file, when="midnight", backupCount=7, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        Log._loggers[logger_name] = logger
        return logger

    def write_update_info_to_logger(self, update_status: str, update_date: str) -> None:
        """
        寫入更新資訊到 log, 確保 update_date 格式為 'YYYY-MM-DD HH:mm:ss'。
        """
        try:
            # 解析並格式化日期
            dt = pendulum.parse(update_date)
            update_date_str = dt.format("YYYY-MM-DD HH:mm:ss")
        except Exception:
            raise ValueError(
                "update_date must be valid by pendulum.parse like 'YYYY-MM-DD HH:mm:ss'"
            )
        update_info = f"{update_status} - {update_date_str}"

        self.logger.info(update_info)

    def get_last_update_date(self) -> str:
        """
        取得 log 檔案中最後一筆由 write_update_info_to_logger 記錄的 update_date。
        只檢查符合 'status - YYYY-MM-DD HH:mm:ss' 格式的行。
        """
        if not self.log_file.exists():
            return ""
        try:
            with self.log_file.open("rb") as f:
                f.seek(0, 2)
                filesize = f.tell()
                blocksize = 1024
                data = b""
                while filesize > 0:
                    seek_size = min(blocksize, filesize)
                    f.seek(-seek_size, 1)
                    data = f.read(seek_size) + data
                    filesize -= seek_size
                    if b"\n" in data:
                        break
                lines = data.decode("utf-8", errors="ignore").splitlines()
                for line in reversed(lines):
                    # 只處理 write_update_info_to_logger 格式: ... - status - YYYY-MM-DD HH:mm:ss
                    parts = line.strip().rsplit(" - ", 1)
                    if len(parts) == 2:
                        date_str = parts[1]
                        try:
                            # 檢查是否為合法日期格式
                            dt = pendulum.from_format(date_str, "YYYY-MM-DD HH:mm:ss")
                            return date_str
                        except Exception:
                            continue
            return ""
        except Exception:
            return ""

    def is_new_update(self, update_date: str) -> bool:
        """
        檢查傳入的 update_date 是否比 log 中最後一筆 update_date 新。
        日期格式需與 log 中一致（如 'YYYY-MM-DD HH:mm:ss'）。
        """
        last_date = self.get_last_update_date()
        if not last_date:
            return True  # 沒有紀錄視為新資料
        try:
            # 使用 pendulum 解析日期字串
            new_dt = pendulum.parse(update_date)
            last_dt = pendulum.parse(last_date)
            return new_dt > last_dt
        except Exception:
            return False  # 格式錯誤視為非新資料
