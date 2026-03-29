import os
from fastmcp import FastMCP
import sqlite3
from prefect import flow

mcp = FastMCP(name="ExpenseTracker")

DB_PATH = os.path.join(os.path.dirname(__file__), "expenses.db")

CATEGORIES_PATH = os.path.join(os.path.dirname(__file__), "categories.json")


def initDB():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS Expenses(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT DEFAULT '',
            note TEXT DEFAULT ''
            )
            """
        )


initDB()


@mcp.tool()
def add_expense(date, amount, category, subcategory="", note=""):
    """Add a new expense entry to the database and add left appropriate details from yourself if not provided"""
    with sqlite3.connect(DB_PATH) as conn:
        curr = conn.execute(
            "INSERT INTO Expenses (date,amount,category,subcategory,note) VALUES (?,?,?,?,?)",
            (date, amount, category, subcategory, note),
        )

        return {"status": "ok", "id": curr.lastrowid}


@mcp.tool()
def list_expense(start_date, end_date):
    "List all the expenses from the list in an inclusive date range"
    with sqlite3.connect(DB_PATH) as conn:
        curr = conn.execute(
            """SELECT id,date,amount,category,subcategory,note 
            FROM Expenses
            WHERE date BETWEEN ? and ?
            ORDER BY id asc
            """,
            (start_date, end_date),
        )
        cols = [d[0] for d in curr.description]
        return [dict(zip(cols, r)) for r in curr.fetchall()]


@mcp.tool()
def summarize(start_date, end_date, category=None):
    """Summarize expenses by category in an inclusive date range"""
    with sqlite3.connect(DB_PATH) as conn:
        query = """
            SELECT category, SUM(amount) AS total_amount
            FROM Expenses
            WHERE date BETWEEN ? and ?
            """

        params = [start_date, end_date]

        if category:
            query += "AND category = ?"
            params.append(category)

        query += "GROUP BY category ORDER BY category ASC"

        curr = conn.execute(query, params)
        cols = [d[0] for d in curr.description]
        return [dict(zip(cols, r)) for r in curr.fetchall()]


@mcp.tool()
def search_tool(query: str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        cursor.execute(
            """SELECT * FROM Expenses
            WHERE CATEGORY LIKE ? OR SUBCATEGORY LIKE ? OR NOTE LIKE ?
            """,
            (f"%{query}%", f"%{query}%", f"%{query}%"),
        )

        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, r)) for r in cursor.fetchall()]


@mcp.tool()
def update_list(
    expense_id: int, amount: float = None, category: str = None, note: str = None
):
    """Updates an existing expense record by whatever the user provides"""

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        query = "UPDATE Expenses SET "
        params = []

        if amount is not None:
            query += "amount = ?,"
            params.append(amount)

        if category is not None:
            query += "category = ?,"
            params.append(category)

        if note is not None:
            query += "note = ?,"
            params.append(note)

        query = query.rstrip(", ")
        query += " WHERE id = ?"
        params.append(expense_id)

        cursor.execute(query, params)
        conn.commit()

        return f"Successfully Updated Expense {expense_id}"


@mcp.tool()
def delete_expense(expense_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        cursor.execute(
            """DELETE FROM Expenses
            WHERE id = ?
            """,
            (expense_id,),
        )

        conn.commit()

        if cursor.rowcount > 0:
            return f"Successfully deleted expense ID {expense_id}."
        else:
            return f"No expense found with ID {expense_id}"


@mcp.resource("expense://categories", mime_type="application/json")
def categories():
    with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        return f.read()


@flow(name="run-server")
def run_server():
    """Entry point for Prefect Cloud deployment"""
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=8000,
    )


if __name__ == "__main__":
    run_server()
