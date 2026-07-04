# Superstore Dataset — Pandas Data Exploration & Cleaning
### Celebal Technologies Internship Assignment

## 📌 Objective
Learn Python basics and perform data exploration and cleaning using Pandas on the
[Superstore Sales Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final).

## 📂 Files in this Submission
| File | Description |
|------|-------------|
| `Celebal_Pandas_Assignment.ipynb` | Jupyter Notebook with all code, explanations, and executed outputs |
| `superstore_cleaned.csv` | Final cleaned dataset after processing |
| `Sample - Superstore.csv` | Original raw dataset (input) |
| `README.md` | This file |

## 🛠️ Steps Performed
1. **Load** — Read `Sample - Superstore.csv` into a Pandas DataFrame (`latin1` encoding).
2. **Explore** — Used `head()`, `tail()`, `shape`, `columns`, `dtypes`, `describe()`, `info()`.
3. **Missing Values** — Checked all columns for nulls; applied median (numeric) / mode
   (categorical) fill logic generically. Dataset was already clean (0 missing values).
4. **Basic Operations** — Selected specific columns; filtered rows by `Category`, `Profit`,
   and `Region` conditions.
5. **Duplicates** — Checked with `duplicated()` and removed with `drop_duplicates()`
   (0 duplicates found in this dataset).
6. **Derived Column** — Created `Unit_Price = Sales / Quantity`, then
   `total_amount = Unit_Price * Quantity`, validated against the original `Sales` column.
7. **Save** — Exported the final cleaned DataFrame to `superstore_cleaned.csv`.

## 📊 Dataset Summary
- **Rows:** 9,994
- **Original columns:** 21
- **Final columns:** 23 (added `Unit_Price`, `total_amount`)
- **Missing values:** 0
- **Duplicate rows:** 0

## ▶️ How to Run
1. Open `Celebal_Pandas_Assignment.ipynb` in Jupyter Notebook / JupyterLab / Google Colab.
2. Ensure `Sample - Superstore.csv` is in the same directory (or update the file path in Step 1).
3. Run all cells sequentially (`Kernel → Restart & Run All`).
4. The cleaned file `superstore_cleaned.csv` will be generated in the same directory.

## 🧰 Tech Stack
- Python 3
- Pandas
- NumPy
- Jupyter Notebook

## ✍️ Author
Harshita — Data Engineering Intern, Celebal Technologies
