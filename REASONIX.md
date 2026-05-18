# REASONIX.md

## Stack
- Python 3 — no version pin
- pandas — Excel data loading and manipulation
- openpyxl — Excel read/write backend
- No framework, no package manager config

## Layout
- `vt4.py` — sole script; `ProductMatcher` class + `__main__` runner
- `подобранные_товары/` — output directory for matched-product spreadsheets
- `logs/` — timestamped per-run logs
- `Ассортимент.xlsx` / `реестр.xlsx` — input data files (hardcoded paths)

## Commands
- **Install**: `pip install pandas openpyxl` (no lockfile, no requirements.txt)
- **Run**: `python vt4.py`
- No build / test / lint / typecheck / format steps exist

## Conventions
- `snake_case` method names
- Single-class stateful design: `ProductMatcher` holds state across load → match → output
- User-facing strings and comments in Russian
- Assortment columns: Бренд, Артикул, Кол-во, Цена за 1 шт
- Registry columns: Пояснение, Сумма

## Watch out for
- README references `vt2.py`; the actual file is `vt4.py` — README is outdated
- Input paths are hardcoded in `__main__`; rename files on disk or update the script
- `.env` contains `DEEPSEEK_API_KEY` but the script never loads or uses it — likely leftover from a prior iteration
