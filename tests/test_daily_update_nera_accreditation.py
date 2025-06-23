from pathlib import Path
from src.tools.log import Log
from dags.daily_update_nera_accreditation import main

down_pdf_path = Path("downloads/nera_accreditation/pdf")
down_csv_path = Path("downloads/nera_accreditation/csv")

save_folder = Path("downloads/nera_accreditation")
if not save_folder.exists():
    save_folder.mkdir(parents=True)
if not (save_folder / "pdf").exists():
    (save_folder / "pdf").mkdir(parents=True)
if not (save_folder / "xlsx").exists():
    (save_folder / "xlsx").mkdir(parents=True)


log_file = save_folder / "update.log"
if not log_file.exists():
    log_file.touch()

if __name__ == "__main__":
    main()
    print("NERA accreditation update completed.")