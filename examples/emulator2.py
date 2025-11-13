# app.py
import sqlite3
from contextlib import closing
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

DB_PATH = "clients.db"
PAY_FREQ = ['Weekly','Biweekly','Semi-Monthly','Monthly']

# -------------- DB helpers --------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    with closing(get_conn()) as conn, conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT NOT NULL,
                assigned_user_id INTEGER NOT NULL,
                assigned_user_name TEXT NOT NULL,
                pay_frequency TEXT NOT NULL CHECK(pay_frequency IN ({str(PAY_FREQ)[1:-1]})),
                pay_start_date TEXT NOT NULL,
                processing_date TEXT NOT NULL,
                pay_date TEXT NOT NULL,
                base_fee REAL NOT NULL,
                num_states_in_base_fee INTEGER NOT NULL,
                num_employees_in_base_fee INTEGER NOT NULL,
                add_state_fee REAL NOT NULL,
                add_employee_fee REAL NOT NULL,
                use_pct_increase INTEGER NOT NULL DEFAULT 1,
                fee_increase_pct REAL NOT NULL,
                fee_increase_effective_date TEXT NOT NULL,
                incr_base_fee REAL NOT NULL,
                incr_add_state_fee REAL NOT NULL,
                incr_add_employee_fee REAL NOT NULL,
                payroll_registration INTEGER NOT NULL CHECK(payroll_registration IN (0,1)),
                registration_fee REAL NOT NULL,
                terminated INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(assigned_user_id) REFERENCES users(id)
            );
            """
        )

        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_terminated ON clients(terminated)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_fee_eff ON clients(fee_increase_effective_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_name ON clients(client_name)")
        
        # to add after client_id:   assigned_employee_id INTEGER NOT NULL,
        # to add at end:            FOREIGN KEY(assigned_employee_id) REFERENCES users(id)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                period_type TEXT NOT NULL CHECK(period_type IN ('regular','additional')),
                pay_frequency TEXT NOT NULL,
                pay_start_date TEXT NOT NULL,
                pay_end_date TEXT NOT NULL,
                processing_date TEXT NOT NULL,
                pay_date TEXT NOT NULL,
                
                base_fee REAL NOT NULL,
                num_states_in_base_fee INTEGER NOT NULL,
                num_employees_in_base_fee INTEGER NOT NULL,
                add_state_fee REAL NOT NULL,
                add_employee_fee REAL NOT NULL,

                num_employees_processed INTEGER NOT NULL,
                num_states_processed INTEGER NOT NULL,

                payroll_registration INTEGER NOT NULL,
                registration_fee REAL NOT NULL,

                registration_this_period INTEGER NOT NULL CHECK(registration_this_period IN (0,1)),
                num_states_registered INTEGER NOT NULL,

                cost REAL NOT NULL,
                collected REAL NOT NULL DEFAULT 0,
                collection_description TEXT NOT NULL DEFAULT '',
                collection_date TEXT,
                net_amount REAL NOT NULL DEFAULT 0,
                FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE
            );
            """
        )

        conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_client ON transactions(client_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_processing ON transactions(processing_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_payend ON transactions(pay_end_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_client_processing ON transactions(client_id, processing_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_client_payend ON transactions(client_id, pay_end_date)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                user_type TEXT NOT NULL CHECK(user_type IN ('Employee','Admin')),
                email TEXT NOT NULL,
                password TEXT,
                permanent INTEGER NOT NULL CHECK(permanent IN (0,1))
            );
            """
        )

def check_active_clients():
    with closing(get_conn()) as conn, conn:
        return bool(conn.execute(f"SELECT EXISTS(SELECT 1 FROM clients WHERE terminated = 0);").fetchone()[0])

def new_entry(table: str, payload: dict):
    cols = ", ".join(payload.keys())
    qmarks = ", ".join(["?"] * len(payload))
    with closing(get_conn()) as conn, conn:
        conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({qmarks})", list(payload.values()))

def fetch_row(table: str, entry_id: int) -> pd.Series:
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table} WHERE id = {entry_id}", conn)
    return df.iloc[0]

def update_entry(table: str, entry_id: int, payload: dict):
    sets = ", ".join([f"{k}=?" for k in payload.keys()])
    values = list(payload.values()) + [entry_id]
    with closing(get_conn()) as conn, conn:
        conn.execute(f"UPDATE {table} SET {sets} WHERE id = ?", values)

def show_clients(terminated: bool, order_by: str = "ID", ascending: bool = True) -> pd.DataFrame:
    select_cols = ["id", "assigned_user_name", "client_name", "pay_frequency", "processing_date", 
                   "pay_date", "fee_increase_effective_date"]
    cols_str = ",".join(select_cols)

    allowed = {
        "ID": "id",
        "Client Name": "client_name COLLATE NOCASE",
        "Assigned User": "assigned_user_name COLLATE NOCASE",
        "Pay Frequency": "pay_frequency COLLATE NOCASE",
        "Processing Date": "date(processing_date)",
        "Pay Date": "date(pay_date)"
    }
    order_expr = allowed.get(order_by, "ID")
    direction = "ASC" if ascending else "DESC"

    with closing(get_conn()) as conn:
        return pd.read_sql_query(f"""SELECT {cols_str} FROM clients WHERE terminated = {int(terminated)} 
                                 ORDER BY {order_expr} {direction}, id ASC""", conn)
    
def get_client_payments(client_id: int, order_field: str, display_cols: list = None) -> pd.DataFrame:
    default_cols = "id, period_type, pay_start_date, pay_end_date, processing_date, cost"
    if display_cols:
        default_cols = ",".join(display_cols)
    with closing(get_conn()) as conn:
        return pd.read_sql_query(f"""SELECT {default_cols}
                                 FROM transactions WHERE client_id = {client_id} ORDER BY {order_field} DESC""", conn)
    
def get_payment_aggregates() -> pd.DataFrame:
    """Per-client sums of cost, collected, net_amount."""
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            """
            SELECT 
                c.id AS client_id, c.client_name,
                COALESCE(SUM(t.cost),0) AS total_cost,
                COALESCE(SUM(t.collected),0) AS total_collected
            FROM clients c
            LEFT JOIN transactions t ON t.client_id = c.id
            WHERE c.terminated = 0
            GROUP BY c.id, c.client_name ORDER BY c.id ASC
            """, conn)
        df["net_amount"] = df["total_cost"] - df["total_collected"]
        return df

def update_client_payments(updates: pd.DataFrame):
    """Updates all payments for this client"""
    all_dates = (updates["collection_date"].str.strip() != "") & updates["collection_date"].notna()
    converted = pd.to_datetime(updates.loc[all_dates, "collection_date"], format="%m/%d/%Y", errors="coerce")
    if not converted.notna().all():
        return "Collection Date must be a valid date in the form of MM/DD/YYYY"
    
    with closing(get_conn()) as conn, conn:
        for _, r in updates.iterrows():
            collect = float(r["collected"] or 0)
            desc = (r.get("collection_description") or "").strip()
            cdate = r.get("collection_date")
            cdate_str = str(cdate) if pd.notna(cdate) and str(cdate) != "" else ''
            net = round(r["cost"] - collect, 2)
            conn.execute(
                """
                UPDATE transactions
                SET collected = ?, collection_description = ?, collection_date = ?, net_amount = ?
                WHERE id = ?
                """,
                (collect, desc, cdate_str, net, int(r["id"])),
            )

def update_users(updates: pd.DataFrame):
    """
    updates: DataFrame with columns: id, user_name, user_type, email, password
    """
    if (~updates['user_type'].str.strip().isin(["Employee", "Admin"])).any():
        return "The User Type must be Employee or Admin"
    if (~updates['email'].str.strip().str.lower().str.endswith("@gmail.com", na=False)).any():
        return "The Email must be in the gmail domain"

    with closing(get_conn()) as conn, conn:
        for _, r in updates.iterrows():
            user_name = (r.get("user_name") or "").strip()
            user_type = (r.get("user_type") or "").strip()
            email = (r.get("email") or "").strip()
            password = (r.get("password") or " ").strip()
            if not email.lower().endswith("@gmail.com"):
                return
            if user_type not in ["Employee", "Admin"]:
                return
            conn.execute(
                f"""UPDATE users SET user_name = '{user_name}', user_type = '{user_type}', email = '{email}', password = '{password}'
                WHERE id = {r["id"]}""")

def get_latest_pay_end_date(client_id: int) -> Optional[date]:
    """Return the latest pay_end_date for this client, or None if no transactions."""
    with closing(get_conn()) as conn:
        last_end_date = conn.execute(f"SELECT MAX(pay_end_date) FROM transactions WHERE client_id = {client_id}").fetchone()[0]
        return date.fromisoformat(last_end_date) if last_end_date else None

def get_client_net_amount(client_id: int) -> float:
    with closing(get_conn()) as conn:
        val = conn.execute(f"SELECT COALESCE(SUM(net_amount), 0) FROM transactions WHERE client_id = {client_id}").fetchone()[0]
        return float(val or 0.0)

def get_users(select_cols: list = None) -> pd.DataFrame:
    default_cols = "*"
    if select_cols:
        default_cols = ",".join(select_cols)
    with closing(get_conn()) as conn:
        return pd.read_sql_query(f"SELECT {default_cols} FROM users", conn)

def delete_user(user_id: int):
        with closing(get_conn()) as conn, conn:
            conn.execute(f"DELETE FROM users WHERE id = {user_id};")

def rollover_fees_for_today(today_iso: str):
    """
    Promote increased fees to current, push effective date +1 year,
    and (if use_pct_increase=1) precompute the next cycle increased fees.
    Only affects active (terminated=0) clients whose effective date is today.
    """
    with closing(get_conn()) as conn, conn:
        conn.execute(
            """
            UPDATE clients
            SET
                fee_increase_effective_date = DATE(fee_increase_effective_date, '+1 year'),
                base_fee                = incr_base_fee,
                add_state_fee           = incr_add_state_fee,
                add_employee_fee        = incr_add_employee_fee,
                incr_base_fee           = ROUND(incr_base_fee * (1.0 + fee_increase_pct/100.0), 2),
                incr_add_state_fee      = ROUND(incr_add_state_fee * (1.0 + fee_increase_pct/100.0), 2),
                incr_add_employee_fee   = ROUND(incr_add_employee_fee * (1.0 + fee_increase_pct/100.0), 2),
                use_pct_increase        = 1
            WHERE terminated = 0 AND fee_increase_effective_date = ?
            """, (today_iso,)
        )


# ---------- Reports ----------
def df_effective_dates() -> pd.DataFrame:
    """Clients fees after their effective dates"""
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT 
                client_name AS "Client Name",
                fee_increase_effective_date AS "Effective Date",
                incr_base_fee          AS "New Base Fee",
                incr_add_state_fee AS "New Additional State Fee",
                incr_add_employee_fee AS "New Additional Employee Fee"
            FROM clients
            ORDER BY client_name ASC
            """, conn
        )
        df["Effective Date"] = pd.to_datetime(df["Effective Date"], errors="coerce").dt.strftime("%m/%d/%Y")
        for col in ["New Base Fee", "New Additional State Fee", "New Additional Employee Fee"]:
            df[col] = df[col].astype(float).round(2)
        return df

def df_collections_overview_range(start_dt: date, end_dt: date) -> pd.DataFrame:
    """
    Aggregates by client over transactions whose processing_date is within [start_dt, end_dt].
    Returns columns: client_name, cost, total_collected, net_amount.
    """
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT 
                c.client_name AS "Client Name",
                COALESCE(SUM(t.cost), 0)        AS "Total Fee",
                COALESCE(SUM(t.collected), 0)   AS "Amount Collected",
                COALESCE(SUM(t.net_amount), 0)  AS "Amount Receivable"
            FROM transactions t
            JOIN clients c ON c.id = t.client_id
            WHERE date(t.processing_date) BETWEEN date('{start_dt.isoformat()}') AND date('{end_dt.isoformat()}')
            GROUP BY c.client_name
            ORDER BY c.client_name ASC
            """, conn
        )
        for col in ["Total Fee", "Amount Collected", "Amount Receivable"]:
            df[col] = df[col].astype(float).round(2)
        return df

def df_num_employees_in_range(start_dt: date, end_dt: date) -> pd.DataFrame:
    """
    Show number of employees processed for each transaction whose processing_date is within [start_dt, end_dt].
    Returns columns: client_name, num_employees_processed.
    """
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT 
                c.client_name AS "Client Name",
                t.processing_date AS "Processed Date",
                t.num_employees_processed AS "Num Employees Processed"
            FROM transactions t
            JOIN clients c ON c.id = t.client_id
            WHERE date(t.processing_date) BETWEEN date('{start_dt.isoformat()}') AND date('{end_dt.isoformat()}')
            ORDER BY c.client_name ASC, t.processing_date ASC
            """, conn
        )
        df["Processed Date"] = pd.to_datetime(df["Processed Date"], errors="coerce").dt.strftime("%m/%d/%Y")
        return df


# ---------- Nav helpers ---------
def set_page(page: str, **kwargs):
    st.query_params.clear()
    st.query_params.update({"page": page, **{k:str(v) for k,v in kwargs.items()}})


# ---------- Utils ----------
def validate_start_date(freq: str, d: date):
    if freq in PAY_FREQ[:2]: # Weekly and Biweekly
        return True, ""
    if freq == PAY_FREQ[2]: # Semi-Monthly
        return (d.day in (1, 16)), "Pay start date must be the 1st or the 16th."
    if freq == PAY_FREQ[3]: # Monthly
        return (d.day == 1), "Pay start date must be the 1st."

def plus_one_year(d: date) -> date:
    return d + timedelta(days=365)

def first_of_next_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 12:
        return date(y + 1, 1, 1)
    return date(y, m + 1, 1)

def advance_pay_start_date(current_start: date, freq: str) -> date:
    if freq == PAY_FREQ[0]: # Weekly
        return current_start + timedelta(days=7)
    elif freq == PAY_FREQ[1]: # Biweekly
        return current_start + timedelta(days=14)
    elif freq == PAY_FREQ[2]: # Semi-Monthly
        if current_start.day == 1:
            return date(current_start.year, current_start.month, 16)
        if current_start.day == 16:
            return first_of_next_month(current_start)
        else:
            st.warning("Pay Start Date has to be the 1st or 16th of the month")
            return None
    elif freq == PAY_FREQ[3]: # Monthly
        if current_start.day == 1:
            return first_of_next_month(current_start)
        else:
            st.warning("Pay Start Date has to be the 1st of the month")
            return None

def pct_increase(val: float, pct: float) -> float:
    return round(val * (1 + (pct / 100.0)), 2)

def calc_cost(base_fee: float, add_state_fee: float, add_employee_fee: float,
              num_states_in_base_fee: int, num_employees_in_base_fee: int, payroll_registration: int,
              registration_fee: float, num_states_processed: int, num_employees_processed: int,
              registration_this_period: bool, num_states_registered: int) -> float:
    extra_states = max(0, num_states_processed - int(num_states_in_base_fee))
    extra_emps = max(0, num_employees_processed - int(num_employees_in_base_fee))

    total = float(base_fee)
    total += extra_states * float(add_state_fee)
    total += extra_emps * float(add_employee_fee)

    if registration_this_period and int(payroll_registration) == 1:
        num_register_states = max(0, num_states_registered - int(num_states_in_base_fee))
        total += num_register_states * registration_fee

    return round(total, 2)

def fade_message():
    st.markdown(
        """
        <style>
        div[data-testid="stAlert"]{ animation: fadeout 0.5s ease 2.5s forwards; }
        @keyframes fadeout { to { opacity: 0;  max-height: 0; padding: 0; margin: 0; overflow: hidden; } }
        </style>
        """, unsafe_allow_html=True)

def maybe_rollover_fees_for_today():
    today_iso = date.today().isoformat()
    if st.session_state.get("last_rollover_date") == today_iso:
        return
    rollover_fees_for_today(today_iso)
    st.session_state["last_rollover_date"] = today_iso
    
# ------- Dialog Screens -------
@st.dialog("Confirm new pay period")
def submit_period_dialog(staged: dict, client: pd.Series):
    st.write("Are you sure you want to submit this pay period?")

    a, b = st.columns([1, 1])
    if a.button("✅ Confirm & Submit", type="primary", width="stretch"):
        new_entry("transactions", staged)

        if staged["period_type"] == "regular":
            start_dt = date.fromisoformat(staged["pay_start_date"])
            next_start = advance_pay_start_date(start_dt, client.pay_frequency)
            next_processing_date = next_start + timedelta(days=(date.fromisoformat(client.processing_date) - start_dt).days)
            next_pay_date = next_start + timedelta(days=(date.fromisoformat(client.pay_date) - start_dt).days)
            update_entry("clients", int(client.id), {"pay_start_date": next_start.isoformat(),
                                                     "processing_date": next_processing_date.isoformat(),
                                                     "pay_date": next_pay_date.isoformat()
            })
            st.session_state["reg_period"] = True
        else:
            st.session_state["add_period"] = True

        set_page("home_detail", id=int(client.id))
        st.rerun()

    if b.button("Cancel", width="stretch"):
        st.rerun()

@st.dialog("Confirm skip this pay period")
def skip_period_dialog(client_id, next_start, next_processing_date, next_pay_date):
    st.write("Are you sure you want to skip this pay period?")

    a, b = st.columns([1, 1])
    if a.button("✅ Skip", type="primary", width="stretch"):
        update_entry("clients", int(client_id), {"pay_start_date": next_start,
                                                 "processing_date": next_processing_date,
                                                 "pay_date": next_pay_date})

        set_page("home_detail", id=int(client_id))
        st.rerun()

    if b.button("Cancel", width="stretch"):
        st.rerun()

@st.dialog("Cannot terminate client")
def cannot_terminate_dialog(client_name: str, net_amt: float):
    st.error(f"**{client_name}** cannot be terminated due to an outstanding balance of **${net_amt:,.2f}**.")
    st.caption(f"Ensure Accounts Receivable is 0 for {client_name} to be able to terminate them.")

@st.dialog("Confirm termination")
def confirm_terminate_dialog(client_id: int, client_name: str):
    st.write(f"Are you sure you want to terminate **{client_name}**?")

    c1, c2 = st.columns(2)
    if c1.button("Yes, terminate", type="primary", key=f"term_yes_{client_id}", width="stretch"):
        update_entry("clients", client_id, {"terminated": 1})
        st.session_state["terminated_msg"] = True
        set_page("main")
        st.rerun()

    if c2.button("Cancel", key=f"term_no_{client_id}", width="stretch"):
        st.rerun()

@st.dialog("Update Fee Increase Effective Date")
def reactivate_dialog(client_id, client_name, current_eff):
    st.write(f"**{client_name}** must have a fee increase effective date after today.")
    new_eff = st.date_input("New Effective Date*", value=date.fromisoformat(current_eff), format="MM/DD/YYYY")

    a, b = st.columns([1, 1])
    if a.button("Save & Reactivate", type="primary", key=f"react_save_{client_id}", width="stretch"):
        if new_eff is None or new_eff <= date.today():
            st.error("New Effective Date must be after today.")
        else:
            update_entry("clients", client_id, {"fee_increase_effective_date": new_eff.isoformat(), "terminated": 0})
            st.session_state["reactivated_msg"] = True
            st.rerun()

    if b.button("Cancel", key=f"react_cancel_{client_id}", width="stretch"):
        st.rerun()

@st.dialog("Confirm user deletion")
def confirm_delete_user_dialog(user_id: int, user_name: str):
    st.write(f"Are you sure you want to delete **{user_name}**?")

    d,e = st.columns(2)
    if d.button("Delete", type="primary", key=f"del_yes_{user_id}", width="stretch"):
        delete_user(user_id)
        st.session_state["deleted_msg"] = True
        st.rerun()

    if e.button("Cancel", key=f"del_no_{user_id}", width="stretch"):
        st.rerun()


# ------- Payroll Screen Helpers -------
def render_period(period_type, client_id):
    st.markdown(f"### {period_type.capitalize()} Pay Period")
    client = fetch_row("clients", client_id)
    
    if period_type == "regular":
        ro1, ro2, ro3 = st.columns(3)
        start_dt = date.fromisoformat(client.pay_start_date)
        next_start = advance_pay_start_date(start_dt, client.pay_frequency)
        end_dt = next_start - timedelta(days=1)
        with ro1: st.text_input("Pay Frequency", value=client.pay_frequency, disabled=True)
        with ro2: st.date_input("Pay Start Date", value=start_dt, disabled=True, format="MM/DD/YYYY")
        with ro3: st.date_input("Pay End Date", value=end_dt, disabled=True, format="MM/DD/YYYY")
    else:
        ro1, ro2 = st.columns(2)
        start_dt = ro1.date_input("Pay Start Date", value=None, format="MM/DD/YYYY")
        end_dt = ro2.date_input("Pay End Date", value=None, format="MM/DD/YYYY")
        next_start = None

    d1, d2 = st.columns(2)
    processing_date = d1.date_input("Processing Date*", value=date.fromisoformat(client.processing_date) if period_type == "regular" else None, format="MM/DD/YYYY")
    pay_date = d2.date_input("Pay Date*", value=date.fromisoformat(client.pay_date) if period_type == "regular" else None, format="MM/DD/YYYY")

    e1, e2 = st.columns(2)
    n_emp = e1.number_input("Number of Employees*", min_value=1, step=1, value=None)
    n_states = e2.number_input("Number of States*", min_value=1, step=1, value=None)

    registration_this_period = False
    num_register_states = 0
    if int(client.payroll_registration) == 1:
        register_choice = st.radio("Register States?", options=["Yes", "No"], index=1, horizontal=True)
        registration_this_period = (register_choice == "Yes")
        if registration_this_period:
            num_register_states = st.number_input("Number of States Registered*", min_value=0, step=1, value=None)

    st.markdown("<br>", unsafe_allow_html=True)
    if period_type == "regular":
        a, b, c = st.columns(3)
        submit_clicked = a.button("Submit", type="primary", width='stretch')
        skip_clicked = b.button("Skip", type="secondary", width='stretch')
        cancel_clicked = c.button("Cancel", width='stretch')
    else:
        a, b = st.columns(2)
        submit_clicked = a.button("Submit", type="primary", width='stretch')
        cancel_clicked = b.button("Cancel", width='stretch')

    if cancel_clicked:
        set_page("home_detail", id=client_id)
        st.rerun()

    if period_type == "regular" and skip_clicked:
        next_processing_date = next_start + timedelta(days=(date.fromisoformat(client.processing_date) - start_dt).days)
        next_pay_date = next_start + timedelta(days=(date.fromisoformat(client.pay_date) - start_dt).days)
        skip_period_dialog(int(client_id), next_start.isoformat(), next_processing_date.isoformat(), next_pay_date.isoformat())

    if submit_clicked:
        errs = []
        if period_type == "additional":
            if start_dt is None:
                errs.append("• Pay Start Date is required.")
            else:
                ok, msg = validate_start_date(client.pay_frequency, start_dt)
                if not ok:
                    errs.append(f"• {msg}")
            if end_dt is None:
                errs.append("• Pay End Date is required.")
            elif start_dt and end_dt <= start_dt:
                errs.append("• Pay End Date should be after Pay Start Date.")
        if processing_date is None:
            errs.append("• Processing Date is required.")
        elif end_dt and processing_date < end_dt:
            errs.append("• Processing Date must be on/after the Pay End Date.")
        if pay_date is None:
            errs.append("• Pay Date is required.")
        elif processing_date and pay_date < processing_date:
            errs.append("• Pay Date must be on/after the Processing Date.")
        if n_emp is None:
            errs.append("• Number of Employees must be > 0.")
        if n_states is None:
            errs.append("• Number of States must be > 0.")
        if registration_this_period and (num_register_states is None or num_register_states <= 0):
            errs.append("• Number of States Registered must be > 0.")

        if errs:
            st.error("Please fix the following:\n\n" + "\n\n".join(errs))
        else:
            snap = {
                "base_fee": float(client.base_fee),
                "add_state_fee": float(client.add_state_fee),
                "add_employee_fee": float(client.add_employee_fee),
                "num_states_in_base_fee": int(client.num_states_in_base_fee),
                "num_employees_in_base_fee": int(client.num_employees_in_base_fee),
                "payroll_registration": int(client.payroll_registration),
                "registration_fee": float(client.registration_fee),
                "num_states_processed": int(n_states),
                "num_employees_processed": int(n_emp),
                "registration_this_period": registration_this_period,
                "num_states_registered": int(num_register_states or 0)
            }
            cost = calc_cost(**snap)
            new_period = {
                "client_id": int(client.id), "period_type": period_type, "pay_frequency": client.pay_frequency,
                "pay_start_date": start_dt.isoformat(), "pay_end_date": end_dt.isoformat(),
                "processing_date": processing_date.isoformat(), "pay_date": pay_date.isoformat(),
                "cost": float(cost), **snap, "collected": 0.0, "collection_description": "", 
                "collection_date": None, "net_amount": float(cost)
            }

            submit_period_dialog(new_period, client)

def render_edit_period(client_id: int, txn_id: int):
    txn = fetch_row("transactions", txn_id)
    st.markdown(f"### Edit {txn.period_type.capitalize()} Pay Period")
    
    if txn.period_type == "regular":
        ro1, ro2, ro3 = st.columns(3)
        start_dt = date.fromisoformat(txn.pay_start_date)
        end_dt = date.fromisoformat(txn.pay_end_date)
        with ro1: st.text_input("Pay Frequency", value=txn.pay_frequency, disabled=True)
        with ro2: st.date_input("Pay Start Date", value=txn.pay_start_date, disabled=True, format="MM/DD/YYYY")
        with ro3: st.date_input("Pay End Date", value=txn.pay_end_date, disabled=True, format="MM/DD/YYYY")
    else:
        ro1, ro2 = st.columns(2)
        start_dt = ro1.date_input("Pay start date*", value=date.fromisoformat(txn.pay_start_date), format="MM/DD/YYYY")
        end_dt = ro2.date_input("Pay end date*", value=date.fromisoformat(txn.pay_end_date), format="MM/DD/YYYY")

    st.markdown("<br>", unsafe_allow_html=True)
    g1, g2, g3 = st.columns(3)
    base_fee = g1.number_input("Base Fee*", min_value=1.0, step=1.0, format="%.2f", value=txn.base_fee)
    num_states = g2.number_input("Number of States in Base Fee*", min_value=0, step=1, value=int(txn.num_states_in_base_fee))
    num_employees = g3.number_input("Number of Employees in Base Fee*", min_value=0, step=1, value=int(txn.num_employees_in_base_fee))
    f1, f2 = st.columns(2)
    add_state_fee = f1.number_input("Additional State Fee*", min_value=1.0, step=1.0, format="%.2f", value=txn.add_state_fee)
    add_emp_fee = f2.number_input("Additional Employee Fee*", min_value=1.0, step=1.0, format="%.2f", value=txn.add_employee_fee)
    st.markdown("<br>", unsafe_allow_html=True)

    e1, e2 = st.columns(2)
    processing_date = e1.date_input("Processing Date*", value=date.fromisoformat(txn.processing_date), format="MM/DD/YYYY")
    pay_date = e2.date_input("Pay Date*", value=date.fromisoformat(txn.pay_date), format="MM/DD/YYYY")
    
    d1, d2 = st.columns(2)
    n_emp = d1.number_input("Number of Employees*", min_value=1, step=1, value=int(txn.num_employees_processed))
    n_states = d2.number_input("Number of States*", min_value=1, step=1, value=int(txn.num_states_processed))

    registration_this_period = bool(int(txn.registration_this_period))

    if int(txn.payroll_registration) == 1:
        register_choice = st.radio("Register States?", options=["Yes", "No"], index=0 if registration_this_period else 1, horizontal=True)
        registration_this_period = (register_choice == "Yes")
        if registration_this_period:
            registration_fee = st.number_input("Registration Fee*", min_value=0.0, step=1.0, format="%.2f", value=float(txn.registration_fee))
            num_register_states = st.number_input("Number of States Registered*", min_value=0, step=1, value=int(txn.num_states_registered))
        else:
            registration_fee = txn.registration_fee
            num_register_states = 0
    else:
        registration_this_period = False
        registration_fee = 0.0
        num_register_states = 0

    st.markdown("<br>", unsafe_allow_html=True)
    a, b = st.columns([1, 1])
    save_clicked = a.button("Save changes", type="primary", width='stretch')
    cancel_clicked = b.button("Cancel", width='stretch')

    if cancel_clicked:
        set_page("home_detail", id=client_id)
        st.rerun()

    if save_clicked:
        errs = []

        if txn.period_type == "additional":
            if start_dt is None:
                errs.append("• Pay Start Date is required.")
            else:
                ok, msg = validate_start_date(txn.pay_frequency, start_dt)
                if not ok:
                    errs.append(f"• {msg}")
            if end_dt is None:
                errs.append("• Pay End Date is required.")
            elif start_dt and end_dt <= start_dt:
                errs.append("• Pay End Date should be after Pay Start Date.")
        if processing_date is None:
            errs.append("• Processing Date is required.")
        elif end_dt and processing_date < end_dt:
            errs.append("• Processing Date must be on/after the Pay End Date.")
        if pay_date is None:
            errs.append("• Pay Date is required.")
        elif processing_date and pay_date < processing_date:
            errs.append("• Pay Date must be on/after the Processing Date.")
        if n_emp is None:
            errs.append("• Number of Employees must be > 0.")
        if n_states is None:
            errs.append("• Number of States must be > 0.")
        if registration_this_period and (num_register_states is None or num_register_states <= 0):
            errs.append("• Number of States Registered must be > 0.")

        if errs:
            st.error("Please fix the following:\n\n" + "\n\n".join(errs))
        else:
            snap = {
                "base_fee": float(base_fee),
                "add_state_fee": float(add_state_fee),
                "add_employee_fee": float(add_emp_fee),
                "num_states_in_base_fee": int(num_states),
                "num_employees_in_base_fee": int(num_employees),
                "num_states_processed": int(n_states),
                "num_employees_processed": int(n_emp),
                "registration_this_period": registration_this_period,
                "registration_fee": float(registration_fee),
                "num_states_registered": int(num_register_states or 0)
            }
            cost = calc_cost(**snap, payroll_registration=txn.payroll_registration)
            update_entry("transactions", txn_id, {"pay_start_date": start_dt.isoformat(), "pay_end_date": end_dt.isoformat(),
                                                  "processing_date": processing_date.isoformat(), "cost": float(cost), **snap})
            st.session_state["edited_txn"] = True
            set_page("home_detail", id=client_id)
            st.rerun()


# -------------- UI --------------
st.set_page_config(page_title="Clients Admin", page_icon="👥", layout="wide")
init_db()
maybe_rollover_fees_for_today()

params = dict(st.query_params)
current_page = params.get("page")

st.title("Clients")
tabs = st.tabs(["🏠 Home", "🛠️ Admin"])

# put this CSS once (e.g., near top of the detail page)
st.markdown("""
<style>
/* make tab label text larger */
div[data-testid="stTabs"] button p {
    font-size: 1.1rem;        /* increase as desired (1.2rem, 18px, etc.) */
    font-weight: 600;         /* optional: make bold */
}

div[data-testid="stButton"] > button {
    white-space: nowrap !important;
    min-width: 70px !important;   /* tweak as you like */
    flex-shrink: 0 !important;
}

.client-name-cell {
  display: flex;
  justify-content: center;    /* centers horizontally */
  align-items: center;        /* centers vertically */
  height: 2.5rem;
  text-align: center;         /* center multi-line text */
  white-space: normal;        /* allow wrapping */
  word-break: break-word;     /* wrap long names */
}
</style>
""", unsafe_allow_html=True)

# -------- Home Tab --------
with tabs[0]:
    home_pages = {"home_main", "home_detail", "home_edit", "new_period", "txn_edit"}
    page = current_page if current_page in home_pages else "home_main"

    if page == "home_main":
        st.subheader("Client List")

        if not check_active_clients():
            if st.session_state.pop("terminated_msg", False):
                st.success("Client Terminated. No active clients. Ask an Admin to assign one.")
            else:
                st.info("No active clients. Ask an Admin to assign one.")
        else:
            if st.session_state.pop("terminated_msg", False):
                st.success("Client Terminated.")
                fade_message()
            
            c_sort, c_dir = st.columns([3, 1])
            with c_sort:
                sort_cols = ["Client Name", "Assigned User", "Pay Frequency", "Processing Date", "Pay Date"]
                sort_label = st.selectbox("Sort by", sort_cols, index=0)
            with c_dir:
                dir_label = st.radio("Direction", ["Ascending", "Descending"], index=0, horizontal=True)

            order_by = sort_label
            ascending = (dir_label == "Ascending")

            df = show_clients(False, order_by=order_by, ascending=ascending)
            
            arrow = " ▲" if ascending else " ▼"
            def hdr_txt(key):
                return f"{key}{arrow}" if order_by == key else key

            header = st.columns((1, 4, 1.5, 1.5, 1.5, 1.5), gap="small")
            header_style = "text-align:center; font-weight:bold; text-align: center;"
            header[1].markdown(f"<p style='{header_style}'>{hdr_txt('Client Name')}</p>", unsafe_allow_html=True)
            header[2].markdown(f"<p style='{header_style}'>{hdr_txt('Assigned User')}</p>", unsafe_allow_html=True)
            header[3].markdown(f"<p style='{header_style}'>{hdr_txt('Pay Frequency')}</p>", unsafe_allow_html=True)
            header[4].markdown(f"<p style='{header_style}'>{hdr_txt('Processing Date')}</p>", unsafe_allow_html=True)
            header[5].markdown(f"<p style='{header_style}'>{hdr_txt('Pay Date')}</p>", unsafe_allow_html=True)
            
            for _, row in df.iterrows():
                cols = st.columns((1, 4, 1.5, 1.5, 1.5, 1.5), gap="small")
                style_css = "display:flex; justify-content:center; align-items:center; height:2.5rem; text-align: center;"

                icon = "⚠️" if (date.fromisoformat(row["fee_increase_effective_date"]) - date.today()).days <= 30 else None
                if cols[0].button("View", key=f"view_{int(row['id'])}", width='stretch', icon=icon):
                    set_page("home_detail", id=int(row["id"]))
                    st.rerun()
                
                cols[1].markdown(f"<p style='{style_css}'>{row['client_name']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='{style_css}'>{row['assigned_user_name']}</p>", unsafe_allow_html=True)
                cols[3].markdown(f"<p style='{style_css}'>{row['pay_frequency']}</p>", unsafe_allow_html=True)
                processing_format = date.fromisoformat(str(row['processing_date'])).strftime('%b %d, %Y')
                cols[4].markdown(f"<p style='{style_css}'>{processing_format}</p>", unsafe_allow_html=True)
                pay_date_format = date.fromisoformat(str(row['pay_date'])).strftime('%b %d, %Y')
                cols[5].markdown(f"<p style='{style_css}'>{pay_date_format}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)


    elif page == "home_detail":
        raw = params.get("id")
        client_id = int(raw[0]) if isinstance(raw, list) else int(raw)
        this_client = fetch_row("clients", client_id)

        if st.session_state.pop("edited_client", False):
            st.success("Client updated successfully.")
            fade_message()
        if st.session_state.pop("reg_period", False):
            st.success("New pay period successfully entered.")
            fade_message()
        if st.session_state.pop("add_period", False):
            st.success("Additional pay period successfully entered.")
            fade_message()
        if st.session_state.pop("edited_txn", False):
            st.success("Pay Period updated successfully.")
            fade_message()

        if st.button("← Back"):
            set_page("home_main")
            st.rerun()

        st.markdown(f"## {this_client.client_name}")

        c1, c2, c3, c4 = st.columns(4)
        if c1.button("Edit Client", width='stretch'):
            set_page("home_edit", id=client_id)
            st.rerun()
        if c2.button("Regular pay period", width='stretch'):
            set_page("new_period", id=client_id, type="regular")
            st.rerun()
        if c3.button("Additional pay period", width='stretch'):
            set_page("new_period", id=client_id, type="additional")
            st.rerun()
        if c4.button("❌ Terminate", width='stretch'):
            total_net = get_client_net_amount(client_id)
            if total_net > 0:
                cannot_terminate_dialog(this_client.client_name, total_net)
            else:
                confirm_terminate_dialog(client_id, this_client.client_name)

        tx_df = get_client_payments(client_id, "processing_date")
        if not tx_df.empty:
            st.markdown("<br><br>", unsafe_allow_html=True)
            st.markdown("### Payment History")

            show_all = st.session_state.get("show_all_txns", False)
            tx_display = tx_df if show_all else tx_df.head(5)

            header = st.columns((0.8, 2, 2, 2, 2, 2), gap="small")
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Period Type</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Start Date</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>End Date</p>", unsafe_allow_html=True)
            header[4].markdown("<p style='text-align:center; font-weight:bold;'>Processed Date</p>", unsafe_allow_html=True)
            header[5].markdown("<p style='text-align:center; font-weight:bold;'>Cost</p>", unsafe_allow_html=True)
            
            for _, row in tx_display.iterrows():
                cols = st.columns((0.8, 2, 2, 2, 2, 2), gap="small")
                style_css = "display:flex; justify-content:center; align-items:center; height:2.5rem;"

                if cols[0].button("Edit", key=f"edit_{int(row['id'])}", width='stretch', icon="✏️"):
                    set_page("txn_edit", id=client_id, txn_id=int(row['id']))
                    st.rerun()

                cols[1].markdown(f"<p style='{style_css}'>{row['period_type']}</p>", unsafe_allow_html=True)
                start_date_format = date.fromisoformat(str(row['pay_start_date'])).strftime('%b %d, %Y')
                cols[2].markdown(f"<p style='{style_css}'>{start_date_format}</p>", unsafe_allow_html=True)
                end_date_format = date.fromisoformat(str(row['pay_end_date'])).strftime('%b %d, %Y')
                cols[3].markdown(f"<p style='{style_css}'>{end_date_format}</p>", unsafe_allow_html=True)
                processing_format = date.fromisoformat(str(row['processing_date'])).strftime('%b %d, %Y')
                cols[4].markdown(f"<p style='{style_css}'>{processing_format}</p>", unsafe_allow_html=True)
                cols[5].markdown(f"<p style='{style_css}'>{'${:,.2f}'.format(float(row['cost']))}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)

            if len(tx_df) > 5:
                label = "Show Less" if show_all else "Show More"
                left, center, right = st.columns([2, 1, 2])
                with center:
                    if st.button(label, width='stretch', key="toggle_txn_table", ):
                        st.session_state["show_all_txns"] = not show_all
                        st.rerun()
    
    elif page == "home_edit":
        st.markdown("### Edit Client")
        raw = params.get("id")
        client_id = int(raw[0]) if isinstance(raw, list) else int(raw)
        row = fetch_row("clients", client_id)

        n1, n2 = st.columns(2)
        name = n1.text_input("Client Name*", value=row.client_name, key=f"edit_name_{client_id}")
        users_df = get_users(["id", "user_name", "user_type"])
        users_df["combined"] = (users_df["user_name"] + " (" + users_df["user_type"] + ")")
        user_idx = int(users_df.index[users_df["id"] == row.assigned_user_id][0]) + 1
        assigned_user = n2.selectbox("Assigned User", ["None"] + users_df["combined"].tolist(), index = user_idx)

        a1, a2, a3 = st.columns(3)
        pay_frequency = a1.selectbox("Pay Frequency*", PAY_FREQ, index=PAY_FREQ.index(row.pay_frequency))
        pay_start_date = a2.date_input("Pay Start Date*", value=date.fromisoformat(row.pay_start_date), format="MM/DD/YYYY")
        next_date = advance_pay_start_date(pay_start_date, pay_frequency) if pay_start_date else None
        pay_end_date = a3.date_input("Pay end Date*", disabled=True, value=next_date - timedelta(days=1) if next_date else None, format="MM/DD/YYYY")
        
        b1, b2 = st.columns(2)
        processing_date = b1.date_input("Processing Date*", value=date.fromisoformat(row.processing_date), format="MM/DD/YYYY")
        pay_date = b2.date_input("Pay Date*", value=date.fromisoformat(row.pay_date), format="MM/DD/YYYY")

        c1, c2, c3 = st.columns(3)
        base_fee = c1.number_input("Base Fee*", min_value=1.0, step=1.0, format="%.2f", value=float(row.base_fee))
        num_states = c2.number_input("Number of States in Base Fee*", min_value=0, step=1, value=int(row.num_states_in_base_fee))
        num_employees = c3.number_input("Number of Employees in Base Fee*", min_value=0, step=1, value=int(row.num_employees_in_base_fee))

        d1, d2 = st.columns(2)
        add_state_fee = d1.number_input("Additional State Fee*", min_value=1.0, step=1.0, format="%.2f", value=float(row.add_state_fee))
        add_emp_fee = d2.number_input("Additional Employee Fee*", min_value=1.0, step=1.0, format="%.2f", value=float(row.add_employee_fee))
        
        method = st.radio("Fee Increase Choice", options=["% Increase", "Manually Change Fee"], horizontal=True,
                          index=0 if row.use_pct_increase == 1 else 1)
        if method == "% Increase":
            use_pct_increase = 1
            e1, e2 = st.columns(2)
            inc_pct = e1.number_input("Fee increase %*", min_value=1.0, step=1.0, format="%.2f", value=float(row.fee_increase_pct))
            eff_date = e2.date_input("Effective Date*", value=date.fromisoformat(row.fee_increase_effective_date), format="MM/DD/YYYY")
            inc_base_override = float(row.incr_base_fee)  # not used when % method is selected
            inc_state_override = float(row.incr_add_state_fee)
            inc_emp_override = float(row.incr_add_employee_fee)
            
        else:
            use_pct_increase = 0
            inc_pct = float(row.fee_increase_pct) # not used when manual change is selected
            w, x, y = st.columns(3)
            inc_base_override = w.number_input("New Base Fee*", min_value=1.0, step=1.0, format="%.2f", value=float(row.incr_base_fee))
            inc_state_override = x.number_input("New Additional State Fee*", min_value=1.0, step=1.0, format="%.2f", 
                                                value=float(row.incr_add_state_fee))
            inc_emp_override = y.number_input("New Additional Employee Fee*", min_value=1.0, step=1.0, format="%.2f", 
                                              value=float(row.incr_add_employee_fee))
            eff_date = st.date_input("Effective Date*", value=date.fromisoformat(row.fee_increase_effective_date), format="MM/DD/YYYY")
        
        register_choice = st.radio("Payroll Registration?", options=["Yes", "No"], horizontal=True, 
                                   index=0 if row.payroll_registration == 1 else 1)
        if register_choice == "Yes": 
            register_fee = st.number_input("Registration Fee*", min_value=0.0, step=1.0, format="%.2f", value=float(row.registration_fee))
        else:
            register_fee = 0.0
        
        a,b = st.columns([1, 1])
        save_clicked = a.button("💾 Save changes", type="primary", key=f"edit_save_{client_id}", width='stretch')
        cancel_clicked = b.button("Cancel", key=f"edit_cancel_{client_id}", width='stretch')

        if cancel_clicked:
            set_page("home_detail", id=client_id)
            st.rerun()

        if save_clicked:
            errors = []
            if not name.strip():
                errors.append("• Client Name is required.")
            if assigned_user == "None":
                errors.append("• Must assign a user to this client.")
            if pay_start_date is None:
                errors.append("• Pay start date is required.")
            else:
                latest_end = get_latest_pay_end_date(client_id)
                if latest_end and pay_start_date <= latest_end:
                    errors.append(f"• Pay start date must be after the latest pay end date ({latest_end.isoformat()}).")
                ok, msg = validate_start_date(pay_frequency, pay_start_date)
                if not ok:
                    errors.append(f"• {msg}")
            if processing_date is None:
                errors.append("• Processing Date is required.")
            elif pay_end_date and processing_date < pay_end_date:
                errors.append("• Processing Date must be on/after Pay End Date.")
            if pay_date is None:
                errors.append("• Pay Date is required.")
            elif processing_date and pay_date < processing_date:
                errors.append("• Pay Date must be on/after Processing Date.")
            if eff_date is None:
                errors.append("• Fee Increase Effective Date is required.")
            elif eff_date <= date.today():
                errors.append("• Fee Increase Effective Date must be after today.")

            reg_bool = (register_choice == "Yes")
            if reg_bool and register_fee <= 0: errors.append("• Registration Fee must be > 0.")
            
            if errors:
                st.error("Please fix the following before saving:\n\n" + "\n\n".join(errors))
            else:
                assigned_user_id = users_df.loc[users_df["combined"] == assigned_user, "id"].iloc[0]
                assigned_user_name = users_df.loc[users_df["combined"] == assigned_user, "user_name"].iloc[0]
                client_info = {
                    "client_name": name.strip().upper(),
                    "assigned_user_id": int(assigned_user_id),
                    "assigned_user_name": assigned_user_name, 
                    "pay_frequency": pay_frequency, 
                    "pay_start_date": pay_start_date.isoformat(),
                    "processing_date": processing_date.isoformat(),
                    "pay_date": pay_date.isoformat(),
                    "base_fee": float(base_fee), 
                    "num_states_in_base_fee": int(num_states), 
                    "num_employees_in_base_fee": int(num_employees), 
                    "add_state_fee": float(add_state_fee), 
                    "add_employee_fee": float(add_emp_fee), 
                    "fee_increase_pct": float(inc_pct), 
                    "payroll_registration": int(reg_bool), 
                    "registration_fee": float(register_fee), 
                    "use_pct_increase": int(use_pct_increase),
                    "fee_increase_effective_date": eff_date.isoformat()
                }

                if use_pct_increase == 1:
                    client_info["incr_base_fee"] = pct_increase(base_fee, inc_pct)
                    client_info["incr_add_state_fee"] = pct_increase(add_state_fee, inc_pct)
                    client_info["incr_add_employee_fee"] = pct_increase(add_emp_fee, inc_pct)
                else:
                    client_info["incr_base_fee"] = round(float(inc_base_override), 2)
                    client_info["incr_add_state_fee"] = round(float(inc_state_override), 2)
                    client_info["incr_add_employee_fee"] = round(float(inc_emp_override), 2)

                update_entry("clients", client_id, client_info)
                st.session_state["edited_client"] = True
                set_page("home_detail", id=client_id)
                st.rerun()
    
    # ---------- NEW PAY PERIOD SCREEN ----------
    elif page == "new_period":
        raw_id = params.get("id")
        raw_type = params.get("type")
        client_id = int(raw_id[0]) if isinstance(raw_id, list) else int(raw_id)
        period_type = str(raw_type[0]) if isinstance(raw_type, list) else str(raw_type)

        render_period(period_type, client_id)

    # ---------- TRANSACTION EDIT SCREEN ----------
    elif page == "txn_edit":
        raw_txn = params.get("txn_id")
        raw_client = params.get("id")
        txn_id = int(raw_txn[0]) if isinstance(raw_txn, list) else int(raw_txn)
        client_id = int(raw_client[0]) if isinstance(raw_client, list) else int(raw_client)
        
        render_edit_period(client_id, txn_id)


# ========== ADMIN TAB ==========
with tabs[1]:
    admin_pages = {"admin_add", "admin_reactivate", "admin_collect", "admin_reports", "add_user", "delete_user", "user_view"}
    page = current_page if current_page in admin_pages else "admin_main"

    if page == "admin_main":
        st.subheader("Admin")
        if st.session_state.pop("new_client", False):
            st.success("Client created successfully.")
            fade_message()
        if st.session_state.pop("new_user", False):
            st.success("New Payroll User created successfully.")
            fade_message()
        if st.session_state.pop("users_saved", False):
            st.success("Users info updated.")
            fade_message()
        if st.session_state.pop("collections_saved", False):
            st.success("Collection details updated.")
            fade_message()

        a, b, c = st.columns(3)
        if a.button("➕ Add new client", width='stretch'):
            set_page("admin_add")
            st.rerun()
        if b.button("♻️ Reactivate Clients", width='stretch'):
            set_page("admin_reactivate")
            st.rerun()
        if c.button("📄 Generate Reports", width='stretch'):
            set_page("admin_reports")
            st.rerun()

        d, e, f = st.columns(3)
        if d.button("➕ Add New User", width='stretch'):
            set_page("add_user")
            st.rerun()
        if e.button("❌ Delete User", width='stretch'):
            set_page("delete_user")
            st.rerun()
        if f.button("Edit User Info", width='stretch'):
            set_page("user_view")
            st.rerun()

        agg = get_payment_aggregates()
        if not agg.empty:
            st.markdown("---")
            st.markdown("### Accounts Receivable")

            header = st.columns((1, 2, 2, 2, 2), gap="small")
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Client Name</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Total Fee</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>Amount Collected</p>", unsafe_allow_html=True)
            header[4].markdown("<p style='text-align:center; font-weight:bold;'>Amount Receivable</p>", unsafe_allow_html=True)

            for _, r in agg.iterrows():
                cols = st.columns((1, 2, 2, 2, 2), gap="small")
                style_css = "display:flex; justify-content:center; align-items:center; height:2.5rem;"

                if cols[0].button("View", key=f"collection_{int(r['client_id'])}"):
                    set_page("admin_collect", id=int(r["client_id"]))
                    st.rerun()

                cols[1].markdown(f"<p style='{style_css}'>{r['client_name']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='{style_css}'>${float(r['total_cost']):,.2f}</p>", unsafe_allow_html=True)
                cols[3].markdown(f"<p style='{style_css}'>${float(r['total_collected']):,.2f}</p>", unsafe_allow_html=True)
                cols[4].markdown(f"<p style='{style_css}'>${float(r['net_amount']):,.2f}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)
    
    
    elif page == "admin_add":
        st.markdown("### New Client")

        st.markdown("""
        <style>
        div[data-testid="stTextInput"] > label > div:first-child,
        div[data-testid="stSelectbox"] > label > div:first-child,
        div[data-testid="stDateInput"] > label > div:first-child,
        div[data-testid="stNumberInput"] > label > div:first-child,
        div[data-testid="stRadio"] > label > div:first-child {
            font-size: 1.15rem !important;    /* increase font size */
            font-weight: 600 !important;      /* make bold */
        }
        </style>
        """, unsafe_allow_html=True)

        n1, n2 = st.columns(2)
        name = n1.text_input("Client Name*",key="client_name", value=None)
        users_df = get_users(["id", "user_name", "user_type"])
        users_df["combined"] = (users_df["user_name"] + " (" + users_df["user_type"] + ")")
        assigned_user = n2.selectbox("Assigned User", ["None"] + users_df["combined"].tolist(), index = 0)

        a1, a2, a3 = st.columns(3)
        pay_frequency = a1.selectbox("Pay frequency*", PAY_FREQ, index=0)
        pay_start_date = a2.date_input("Pay Start Date*", value=None, format="MM/DD/YYYY")
        next_date = advance_pay_start_date(pay_start_date, pay_frequency) if pay_start_date else None
        pay_end_date = a3.date_input("Pay end Date*", disabled=True, value=next_date - timedelta(days=1) if next_date else None, format="MM/DD/YYYY")
        
        b1, b2 = st.columns(2)
        processing_date = b1.date_input("Processing Date*", value=None, format="MM/DD/YYYY")
        pay_date = b2.date_input("Pay Date*", value=None, format="MM/DD/YYYY")

        c1, c2, c3 = st.columns(3)
        base_fee = c1.number_input("Base Fee*", min_value=1.0, step=1.0, format="%.2f", value=None)
        num_states = c2.number_input("Number of States in Base Fee*", min_value=0, step=1, value=None)
        num_employees = c3.number_input("Number of Employees in Base Fee*",  min_value=0, step=1, value=None)

        d1, d2, d3 = st.columns(3)
        add_state_fee = d1.number_input("Additional State Fee*", min_value=1.0, step=1.0, format="%.2f", value=None)
        add_emp_fee = d2.number_input("Additional Employee Fee*", min_value=1.0, step=2.0, format="%.2f", value=None)
        inc_pct = d3.number_input("Fee increase %*", min_value=1.0, step=1.0, format="%.2f", value=None)

        register_choice = st.radio("Payroll Registration?", options=["Yes", "No"], horizontal=True, index=1)
        if register_choice == "Yes":
            register_fee = st.number_input("Registration Fee*", min_value=0.0, step=1.0, format="%.2f", value=None)
        else:
            register_fee = 0.00

        a_col, b_col = st.columns([1, 1])
        create_clicked = a_col.button("Create Client", type="primary", width='stretch')
        cancel_clicked = b_col.button("Cancel", width='stretch')

        if cancel_clicked:
            set_page("admin_main")
            st.rerun()

        if create_clicked:
            errors = []
            if not name:
                errors.append("• Client Name is required.")
            if assigned_user == "None":
                errors.append("• Must assign a user to this client.")
            if pay_start_date is None:
                errors.append("• Pay Start Date is required.")
            else:
                ok, msg = validate_start_date(pay_frequency, pay_start_date)
                if not ok:
                    errors.append(f"• {msg}")
            if processing_date is None:
                errors.append("• Processing Date is required.")
            elif pay_end_date and processing_date < pay_end_date:
                errors.append("• Processing Date must be on/after Pay End Date.")
            if pay_date is None:
                errors.append("• Pay Date is required.")
            elif processing_date and pay_date < processing_date:
                errors.append("• Pay Date must be on/after Processing Date.")
            
            if base_fee is None: errors.append("• Base Fee must be a number > 0.")
            if num_states is None: errors.append("• Number of states in base fee must be a number >= 0.")
            if num_employees is None: errors.append("• Number of employees in base fee must be a number >= 0.")
            if add_state_fee is None: errors.append("• Additional state fee must be a number > 0.")
            if add_emp_fee is None: errors.append("• Additional employee fee must be a number > 0.")
            if inc_pct is None: errors.append("• Fee Increase % must be a number > 0.")

            reg_bool = (register_choice == "Yes")
            if reg_bool and register_fee <= 0: errors.append("• Registration Fee must be > 0.")
            
            if errors:
                st.error("Please fix the following before creating a new client:\n\n" + "\n\n".join(errors))
            else:
                assigned_user_id = users_df.loc[users_df["combined"] == assigned_user, "id"].iloc[0]
                assigned_user_name = users_df.loc[users_df["combined"] == assigned_user, "user_name"].iloc[0]
                new_client = {
                    "client_name": name.upper(),
                    "assigned_user_id": int(assigned_user_id),
                    "assigned_user_name": assigned_user_name,
                    "pay_frequency": pay_frequency,
                    "pay_start_date": pay_start_date.isoformat(),
                    "processing_date": processing_date.isoformat(),
                    "pay_date": pay_date.isoformat(),
                    "base_fee": base_fee,
                    "num_states_in_base_fee": num_states,
                    "num_employees_in_base_fee": num_employees,
                    "add_state_fee": add_state_fee,
                    "add_employee_fee": add_emp_fee,
                    "fee_increase_pct": inc_pct,
                    "use_pct_increase": 1,
                    "payroll_registration": int(reg_bool),
                    "registration_fee": register_fee,
                    "fee_increase_effective_date": plus_one_year(pay_start_date).isoformat(),
                    "incr_base_fee": pct_increase(base_fee, inc_pct),
                    "incr_add_state_fee": pct_increase(add_state_fee, inc_pct),
                    "incr_add_employee_fee": pct_increase(add_emp_fee, inc_pct)
                }

                new_entry("clients", new_client)
                st.session_state["new_client"] = True  # non-widget key, safe to set
                set_page("admin_main")            # back to Admin initial view
                st.rerun()


    elif page == "admin_reactivate":
        if st.button("← Back"):
            set_page("admin_main")
            st.rerun()

        st.markdown("### Terminated Clients")

        df = show_clients(True)

        if df.empty:
            if st.session_state.pop("reactivated_msg", False):
                st.success("Client reactivated. No terminated clients now.")
            else:
                st.info("No terminated clients.")
        else:
            if st.session_state.pop("reactivated_msg", False):
                st.success("Client reactivated.")
                fade_message()
            st.markdown("<br><br>", unsafe_allow_html=True)

            header = st.columns((1, 2, 2, 2, 2), gap="small")
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Client Name</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Pay Frequency</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>Processing Date</p>", unsafe_allow_html=True)
            header[4].markdown("<p style='text-align:center; font-weight:bold;'>Pay Date</p>", unsafe_allow_html=True)

            for _, row in df.iterrows():
                cols = st.columns((1, 2, 2, 2, 2), gap="small")
                style_css = "display:flex; justify-content:center; align-items:center; height:2.5rem;"
                
                if cols[0].button("Reactivate", key=f"react_{int(row['id'])}"):
                    if date.fromisoformat(row["fee_increase_effective_date"]) <= date.today():
                        reactivate_dialog(int(row["id"]), row['client_name'], row["fee_increase_effective_date"])
                    else:
                        update_entry("clients", int(row["id"]), {"terminated": 0})
                        st.session_state["reactivated_msg"] = True
                        st.rerun()

                cols[1].markdown(f"<p style='{style_css}'>{row['client_name']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='{style_css}'>{row['pay_frequency']}</p>", unsafe_allow_html=True)
                processing_format = date.fromisoformat(str(row['processing_date'])).strftime('%b %d, %Y')
                cols[3].markdown(f"<p style='{style_css}'>{processing_format}</p>", unsafe_allow_html=True)
                pay_date_format = date.fromisoformat(str(row['pay_date'])).strftime('%b %d, %Y')
                cols[4].markdown(f"<p style='{style_css}'>{pay_date_format}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)
    

    elif page == "admin_collect":
        raw = params.get("id")
        client_id = int(raw[0]) if isinstance(raw, list) else int(raw)
        client = fetch_row("clients", client_id)

        st.markdown(f"### Collections for {client.client_name}")

        display_cols = ["id", "pay_start_date", "pay_end_date", "cost", "collected",
                        "collection_description", "collection_date"]
        df = get_client_payments(client_id, "processing_date", display_cols)
        if df.empty:
            if st.button("← Back"):
                set_page("admin_main")
                st.rerun()
            st.info(f"No amount receivable for {client.client_name}.")
        else:
            display_df = df.drop(columns=["id"])

            left, right = st.columns([6, 1], gap=None)
            edited = left.data_editor(
                display_df, width='stretch', hide_index=True, 
                column_config={
                    "pay_start_date": st.column_config.DateColumn("Pay Start date", disabled=True, format="localized"),
                    "pay_end_date": st.column_config.DateColumn("Pay End date", disabled=True, format="localized"),
                    "cost": st.column_config.NumberColumn("Total Fee", disabled=True, format="dollar"),
                    "collected": st.column_config.NumberColumn("Total Collected", min_value=0.00, step=0.01, format="dollar", default=None),
                    "collection_description": st.column_config.TextColumn("Collection Description"),
                    "collection_date": st.column_config.TextColumn("Collection Date", max_chars=10, help="Enter date as MM/DD/YYYY",
                                                                   validate=r"^$|^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/[0-9]{4}$")
                },
                key=f"admin_collect_editor_{client_id}"
            )
            edited["Amount Receivable"] = (edited["cost"].astype(float) - edited["collected"].fillna(0).astype(float)).round(2)
            right.dataframe(edited["Amount Receivable"].map("${:,.2f}".format), hide_index=True)

            edited["id"] = df["id"].values

            a, b = st.columns([1,1])
            save_clicked = a.button("Save changes", type="primary", width='stretch')
            cancel_clicked = b.button("Cancel", width='stretch')

            if cancel_clicked:
                set_page("admin_main")
                st.rerun()

            if save_clicked:
                error = update_client_payments(edited)
                if error:
                    st.error(error)
                else:
                    st.session_state["collections_saved"] = True
                    set_page("admin_main")
                    st.rerun()


    elif page == "add_user":
        st.markdown("### New User")

        a, b, c = st.columns(3)
        name = a.text_input("Name*", value=None)
        email = b.text_input("Email Address (login username)*", value=None)
        user_type = c.selectbox("User Type*", ["Employee", "Admin"], index=0)

        a_col, b_col = st.columns([1, 1])
        create_clicked = a_col.button("Create Employee", type="primary", width='stretch')
        cancel_clicked = b_col.button("Cancel", width='stretch')

        if cancel_clicked:
            set_page("admin_main")
            st.rerun()

        if create_clicked:
            errors = []
            if not name:
                errors.append("• Name is required.")
            if not email:
                errors.append("• Email Address is required.")
            elif not email.strip().lower().endswith("@gmail.com"):
                errors.append("• Email Address must be in the 'gmail' domain")
            
            if errors:
                st.error("Please fix the following before creating a new user:\n\n" + "\n\n".join(errors))
            else:
                new_user = {
                    "user_name": name,
                    "user_type": user_type,
                    "email": email,
                    "password": '',
                    'permanent': 0
                }

                new_entry("users", new_user)
                st.session_state["new_user"] = True  # non-widget key, safe to set
                set_page("admin_main")              # back to Admin initial view
                st.rerun()


    elif page == "delete_user":
        if st.button("← Back"):
            set_page("admin_main")
            st.rerun()
        
        st.markdown("### Delete User")

        if st.session_state.pop("deleted_msg", False):
            st.success("Deleted a user")
            fade_message()
        
        header = st.columns((1, 2, 2, 2), gap="small")
        header[1].markdown("<p style='text-align:center; font-weight:bold;'>User Name</p>", unsafe_allow_html=True)
        header[2].markdown("<p style='text-align:center; font-weight:bold;'>User Type</p>", unsafe_allow_html=True)
        header[3].markdown("<p style='text-align:center; font-weight:bold;'>Email Address</p>", unsafe_allow_html=True)

        users_df = get_users()
        for _, row in users_df.iterrows():
            cols = st.columns((1, 2, 2, 2), gap="small")
            style_css = "display:flex; justify-content:center; align-items:center; height:2.5rem;"
            
            with cols[0]:
                disabled_status = True if int(row['permanent']) == 1 else False
                help_status = "Permanent user, cannot delete" if int(row['permanent']) == 1 else None

                if st.button("Delete", key=f"react_{int(row['id'])}", disabled=disabled_status, help=help_status):
                    confirm_delete_user_dialog(int(row["id"]), row["user_name"])

            cols[1].markdown(f"<p style='{style_css}'>{row['user_name']}</p>", unsafe_allow_html=True)
            cols[2].markdown(f"<p style='{style_css}'>{row['user_type']}</p>", unsafe_allow_html=True)
            cols[3].markdown(f"<p style='{style_css}'>{row['email']}</p>", unsafe_allow_html=True)
            st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)
        
    
    elif page == "user_view":
        if st.button("← Back"):
            set_page("admin_main")
            st.rerun()

        st.markdown(f"### User Information")

        users_df = get_users()
        ids = users_df["id"]
        users_df = users_df.drop(columns=["id"])
        edited = st.data_editor(users_df, width='stretch', hide_index=True,
                                column_config={
                                    "user_name": st.column_config.TextColumn("Name"),
                                    "user_type": st.column_config.TextColumn("User Type"),
                                    "email": st.column_config.TextColumn("Email Address"),
                                    "password": st.column_config.TextColumn("Password"),
                                }, key=f"admin_user_editor"
        )
        edited["id"] = ids.values

        a, b = st.columns([1,1])
        save_clicked = a.button("Save changes", type="primary", width='stretch')
        cancel_clicked = b.button("Cancel", width='stretch')

        if cancel_clicked:
            set_page("admin_main")
            st.rerun()

        if save_clicked:
            errors = update_users(edited)
            if errors:
                st.error(f"Please fix the following before updating user info:\n\n • {errors}")
            else:
                st.session_state["users_saved"] = True
                set_page("admin_main")
                st.rerun()


    elif page == "admin_reports":
        if st.button("← Back"):
            set_page("main")
            st.rerun()

        st.markdown("### Reports")

        st.markdown("#### 1) Fee Increase Effective Date Report")
        col3, col4 = st.columns([1, 4])
        with col3:
            df_fut = df_effective_dates()
            csv_fut = df_fut.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_fut,
                file_name="effective_date_report.csv",
                mime="text/csv",
                key="dl_eff_date",
            )
        with col4:
            if not df_fut.empty:
                st.dataframe(df_fut.style.format({"New Base Fee": "${:.2f}", "New Additional State Fee": "${:.2f}",  
                                                  "New Additional Employee Fee": "${:.2f}"}),
                             width='stretch', hide_index=True, height=220)

        st.markdown("---")

        st.markdown("#### 2) Collections overview (by processing date range)")
        f1, f2, f3 = st.columns(3, vertical_alignment="bottom")
        with f1:
            range_start = st.date_input("Range start*", value=None, key="rep_range_start", format="MM/DD/YYYY")
        with f2:
            range_end = st.date_input("Range end*", value=None, key="rep_range_end", format="MM/DD/YYYY")
        
        valid = (range_start is not None) and (range_end is not None) and (range_end >= range_start)
        file_name = ""
        csv_bytes = None
        if valid:
            range_overview_df = df_collections_overview_range(range_start, range_end)
            csv_bytes = range_overview_df.to_csv(index=False).encode("utf-8")
            file_name = f"collections_overview_{range_start.isoformat()}_to_{range_end.isoformat()}.csv"
        
        with f3:
            st.download_button(
                label="Generate CSV",
                data=(csv_bytes or b""), 
                file_name=file_name,
                mime="text/csv",
                disabled=not valid,
                help="Enter a valid start/end date to generate a CSV.",
                key="dl_collections_range_onebtn",
                width="stretch", type="primary"
            )
        if valid and not range_overview_df.empty:
            st.dataframe(range_overview_df.style.format({"Total Fee": "${:.2f}", "Amount Collected": "${:.2f}", "Amount Receivable": "${:.2f}"}),
                         width="stretch", hide_index=True)
        
        st.markdown("---")

        st.markdown("#### 3) Number of employees processed in each payment period for all clients (by processing date range)")
        g1, g2, g3 = st.columns(3, vertical_alignment="bottom")
        with g1:
            range_start = st.date_input("Range start*", value=None, key="ee_range_start", format="MM/DD/YYYY")
        with g2:
            range_end = st.date_input("Range end*", value=None, key="ee_range_end", format="MM/DD/YYYY")
        
        ee_valid = (range_start is not None) and (range_end is not None) and (range_end >= range_start)
        file_ee_name = ""
        csv_ee_bytes = None
        if ee_valid:
            employees_df = df_num_employees_in_range(range_start, range_end)
            csv_ee_bytes = employees_df.to_csv(index=False).encode("utf-8")
            file_ee_name = f"employees_processed_{range_start.isoformat()}_to_{range_end.isoformat()}.csv"
        
        with g3:
            st.download_button(
                label="Generate CSV",
                data=(csv_ee_bytes or b""), 
                file_name=file_ee_name,
                mime="text/csv",
                disabled=not ee_valid,
                help="Enter a valid start/end date to generate a CSV.",
                key="dl_ee_range_onebtn",
                width="stretch", type="primary"
            )
        if ee_valid and not employees_df.empty:
            st.dataframe(employees_df, width="stretch", hide_index=True)
