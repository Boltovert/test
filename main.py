import os
import pandas as pd
import dateparser
from datetime import datetime
from multiprocessing import Pool, cpu_count
import time

INPUT_FILE = "test_data.csv"
OUTPUT_FILE = "normalized_dates.csv"
REPORT_FILE = "normalization_report.txt"


def normalize_date(date_str: str) -> str | None:
    if not isinstance(date_str, str) or not date_str.strip():
        return None

    parsed = dateparser.parse(
        date_str,
        languages=["ru"],
        settings={
            "DATE_ORDER": "DMY",
            "PREFER_DAY_OF_MONTH": "first",
            "RELATIVE_BASE": datetime(1900, 1, 1),
        },
    )
    if parsed is None:
        return None

    return parsed.strftime("%d-%m-%Y")


def save_report(
    total_rows,
    converted_count,
    failed_count,
    changed_examples,
    error_examples,
    execution_time,
):
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("=== Отчёт о нормализации ===\n")
        f.write(f"Всего строк: {total_rows}\n")
        f.write(f"Успешно нормализовано: {converted_count}\n")
        f.write(f"Ошибок: {failed_count}\n")
        f.write("\nПримеры изменений:\n")
        for i, (orig, norm) in enumerate(changed_examples, 1):
            f.write(f"{i}: '{orig}' → '{norm}'\n")

        f.write("\n=== Примеры строк, которые не удалось распарсить ===\n")
        if error_examples:
            for i, err_str in enumerate(error_examples, 1):
                f.write(f"{i}: '{err_str}'\n")
        else:
            f.write("Ошибок парсинга нет.\n")

        f.write(f"\nВремя выполнения скрипта: {execution_time:.2f} секунд\n")


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Файл {INPUT_FILE} не найден.")
        return

    chunksize = 100_000
    pool = Pool(processes=cpu_count())

    total_rows = 0
    converted_count = 0
    failed_count = 0
    changed_examples = []
    error_examples = []

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    if os.path.exists(REPORT_FILE):
        os.remove(REPORT_FILE)

    start_time = time.time()

    with pd.read_csv(INPUT_FILE, chunksize=chunksize, encoding="utf-8") as reader:
        for chunk_idx, chunk in enumerate(reader):
            col = chunk.columns[0]
            dates = chunk[col].fillna("").tolist()

            normalized_dates = pool.map(normalize_date, dates)

            normalized_dates_fixed = []
            for orig, norm in zip(dates, normalized_dates):
                if orig == "":
                    normalized_dates_fixed.append("")
                elif norm is None:
                    normalized_dates_fixed.append("Invalid")
                else:
                    normalized_dates_fixed.append(norm)

            total_rows += len(dates)
            for orig, norm in zip(dates, normalized_dates_fixed):
                if norm not in ("", "Invalid"):
                    converted_count += 1
                    if orig != norm and len(changed_examples) < 5:
                        changed_examples.append((orig, norm))
                elif norm == "Invalid":
                    failed_count += 1
                    if len(error_examples) < 5:
                        error_examples.append(orig)

            header = chunk_idx == 0
            df_out = pd.DataFrame({col: normalized_dates_fixed})
            df_out.to_csv(
                OUTPUT_FILE, mode="a", index=False, encoding="utf-8", header=header
            )

    pool.close()
    pool.join()

    end_time = time.time()
    execution_time = end_time - start_time

    save_report(
        total_rows,
        converted_count,
        failed_count,
        changed_examples,
        error_examples,
        execution_time,
    )

    with open(REPORT_FILE, "r", encoding="utf-8") as f:
        print(f.read())


if __name__ == "__main__":
    main()
