# app.py
import sqlite3
from contextlib import closing
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

DB_PATH = "clients.db"

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
                pay_frequency TEXT NOT NULL CHECK(pay_frequency IN ('weekly','semi-weekly','semi-monthly','monthly')),
                pay_start_date TEXT NOT NULL,
                base_fee REAL NOT NULL,
                num_states_in_base_fee INTEGER NOT NULL,
                num_employees_in_base_fee INTEGER NOT NULL,
                additional_state_fee REAL NOT NULL,
                additional_employee_fee REAL NOT NULL,
                use_pct_increase INTEGER NOT NULL DEFAULT 1,
                fee_increase_pct REAL NOT NULL,
                fee_increase_effective_date TEXT NOT NULL,
                increased_base_fee REAL NOT NULL,
                increased_additional_state_fee REAL NOT NULL,
                increased_additional_employee_fee REAL NOT NULL,
                help_registration INTEGER NOT NULL CHECK(help_registration IN (0,1)),
                help_fee REAL NOT NULL,
                terminated INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        
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
                base_fee REAL NOT NULL,
                num_states_in_base_fee INTEGER NOT NULL,
                num_employees_in_base_fee INTEGER NOT NULL,
                additional_state_fee REAL NOT NULL,
                additional_employee_fee REAL NOT NULL,
                num_employees_processed INTEGER NOT NULL,
                num_states_processed INTEGER NOT NULL,
                help_registration INTEGER NOT NULL,
                help_fee REAL NOT NULL,
                help_this_period INTEGER NOT NULL CHECK(help_this_period IN (0,1)),
                num_states_helped INTEGER NOT NULL,
                cost REAL NOT NULL,
                collected REAL NOT NULL DEFAULT 0,
                collection_description TEXT NOT NULL DEFAULT '',
                collection_date TEXT,
                net_amount REAL NOT NULL DEFAULT 0,
                FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE
            );
            """
        )

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

def show_clients(select_cols: list, terminated: bool) -> pd.DataFrame:
    cols_str = ",".join(select_cols)
    with closing(get_conn()) as conn:
        return pd.read_sql_query(f"SELECT {cols_str} FROM clients WHERE terminated = {int(terminated)} ORDER BY id ASC", conn)
    
def fetch_client_transactions(client_id: int, order_field: str) -> pd.DataFrame:
    with closing(get_conn()) as conn:
        return pd.read_sql_query(f"""SELECT id, period_type, pay_start_date, pay_end_date, processing_date, cost
                                 FROM transactions WHERE client_id = {client_id} ORDER BY {order_field} DESC""", conn)

def fetch_admin_aggregates() -> pd.DataFrame:
    """Per-client sums of cost, collected, net_amount."""
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT 
                c.id AS client_id, c.client_name,
                COALESCE(SUM(t.cost),0) AS cost,
                COALESCE(SUM(t.collected),0) AS total_collected,
                COALESCE(SUM(t.net_amount),0) AS net_amount
            FROM clients c
            LEFT JOIN transactions t ON t.client_id = c.id
            WHERE c.terminated = 0
            GROUP BY c.id, c.client_name ORDER BY c.id ASC
            """, conn)

def fetch_transactions_for_admin_edit(client_id: int) -> pd.DataFrame:
    """Rows/columns the admin needs to edit collections."""
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            f"""
            SELECT  id, pay_start_date, pay_end_date, cost, collected, 
                    collection_description, collection_date, net_amount
            FROM transactions WHERE client_id = {client_id} ORDER BY processing_date DESC
            """, conn)


def update_transaction_collection_fields(updates: pd.DataFrame):
    """
    updates: DataFrame with columns: id, collected, collection_description, collection_date, cost
    Recomputes net_amount = cost - collected.
    """
    with closing(get_conn()) as conn, conn:
        for _, r in updates.iterrows():
            coll = float(r["collected"] or 0)
            desc = (r.get("collection_description") or "").strip()
            cdate = r.get("collection_date")
            cdate_str = None
            if pd.notna(cdate) and str(cdate) != "":
                try:
                    cdate_str = pd.to_datetime(cdate).date().isoformat()
                except Exception:
                    cdate_str = None
            total = float(r["cost"] or 0)
            net = round(total - coll, 2)
            conn.execute(
                """
                UPDATE transactions
                SET collected = ?, collection_description = ?, collection_date = ?, net_amount = ?
                WHERE id = ?
                """, (coll, desc, cdate_str, net, int(r["id"])) )

def fetch_latest_pay_end_date(client_id: int) -> Optional[date]:
    """Return the latest pay_end_date for this client, or None if no transactions."""
    with closing(get_conn()) as conn:
        row = conn.execute(f"SELECT MAX(pay_end_date) FROM transactions WHERE client_id = {client_id}").fetchone()
        if not row or row[0] is None:
            return None
        return date.fromisoformat(row[0])

def fetch_client_total_net_amount(client_id: int) -> float:
    with closing(get_conn()) as conn:
        val = conn.execute(f"SELECT COALESCE(SUM(net_amount), 0) FROM transactions WHERE client_id = {client_id}").fetchone()[0]
        return float(val or 0.0)
    
# ---------- Reports ----------
def df_clients_fee_increase_past() -> pd.DataFrame:
    """Clients with fee increase effective dates in the past (strictly before today)."""
    today = date.today().isoformat()
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT 
                client_name,
                fee_increase_effective_date AS previous_effective_date,
                base_fee          AS current_base_fee,
                additional_state_fee AS current_add_state_fee,
                additional_employee_fee AS current_add_employee_fee
            FROM clients
            WHERE date(fee_increase_effective_date) < date(?)
            ORDER BY client_name ASC
            """,
            conn,
            params=(today,),
        )

def df_clients_fee_increase_future() -> pd.DataFrame:
    """Clients with fee increase effective dates in the future (strictly after today)."""
    today = date.today().isoformat()
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT 
                client_name,
                fee_increase_effective_date AS future_effective_date,
                increased_base_fee          AS increased_base_fee,
                increased_additional_state_fee AS increased_add_state_fee,
                increased_additional_employee_fee AS increased_add_employee_fee
            FROM clients
            WHERE date(fee_increase_effective_date) > date(?)
            ORDER BY client_name ASC
            """,
            conn,
            params=(today,),
        )

def df_collections_overview_range(start_dt: date, end_dt: date) -> pd.DataFrame:
    """
    Aggregates by client over transactions whose processing_date is within [start_dt, end_dt].
    Returns columns: client_name, cost, total_collected, net_amount.
    """
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT 
                c.client_name,
                COALESCE(SUM(t.cost), 0)   AS cost,
                COALESCE(SUM(t.collected), 0)    AS total_collected,
                COALESCE(SUM(t.net_amount), 0)   AS net_amount
            FROM transactions t
            JOIN clients c ON c.id = t.client_id
            WHERE date(t.processing_date) >= date(?)
              AND date(t.processing_date) <= date(?)
            GROUP BY c.client_name
            ORDER BY c.client_name ASC
            """,
            conn,
            params=(start_dt.isoformat(), end_dt.isoformat()),
        )
# ---------- Nav helpers ----------
def set_page(page: str, **kwargs):
    st.session_state.pop("terminate_open", None)
    st.session_state.pop("terminate_client_id", None)
    st.session_state.pop("show_cannot_terminate", None)
    st.session_state.pop("cannot_term_client_id", None)
    
    st.query_params.clear()
    st.query_params.update({"page": page, **{k:str(v) for k,v in kwargs.items()}})

# ---------- Utils ----------
def validate_start_date(freq: str, d: date):
    if freq in ("weekly", "semi-weekly"):
        return True, ""
    if freq == "semi-monthly":
        return (d.day in (1, 16)), "Period start date must be the 1st or the 16th."
    if freq == "monthly":
        return (d.day == 1), "Period start date must be the 1st."

def plus_one_year(d: date) -> date:
    return d + timedelta(days=365)

def first_of_next_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 12:
        return date(y + 1, 1, 1)
    return date(y, m + 1, 1)

def advance_pay_start_date(current_start: date, freq: str) -> date:
    if freq == "weekly":
        return current_start + timedelta(days=7)
    elif freq == "semi-weekly":
        return current_start + timedelta(days=14)
    elif freq == "semi-monthly":
        if current_start.day == 1:
            return date(current_start.year, current_start.month, 16)
        if current_start.day == 16:
            return first_of_next_month(current_start)
    elif freq == "monthly":
        return first_of_next_month(current_start)

def pct_increase(val: float, pct: float) -> float:
    return round(val * (1 + (pct / 100.0)), 2)

def calc_cost(base_fee: float,
                    additional_state_fee: float,
                    additional_employee_fee: float,
                    num_states_in_base_fee: int,
                    num_employees_in_base_fee: int,
                    help_registration: int,
                    help_fee: float,
                    num_states_processed: int,
                    num_employees_processed: int,
                    help_this_period: bool,
                    num_states_helped: int) -> float:
    # Base + extras (no discounts for being under base; clamp extras at 0)
    extra_states = max(0, num_states_processed - int(num_states_in_base_fee))
    extra_emps = max(0, num_employees_processed - int(num_employees_in_base_fee))

    total = float(base_fee)
    total += extra_states * float(additional_state_fee)
    total += extra_emps * float(additional_employee_fee)

    if help_this_period and int(help_registration) == 1:
        extra_help_states = max(0, num_states_helped - int(num_states_in_base_fee))
        total += extra_help_states * float(help_fee)

    return round(total, 2)

def render_period(period_type, client_id):
    st.markdown(f"### {period_type.capitalize()} Pay Period")
    client = fetch_row("clients", client_id)

    
    if period_type == "regular":
        ro1, ro2, ro3 = st.columns(3)
        start_dt = date.fromisoformat(client.pay_start_date)
        next_start = advance_pay_start_date(start_dt, client.pay_frequency)
        end_dt = next_start - timedelta(days=1)
        with ro1: st.text_input("Pay frequency", value=client.pay_frequency, disabled=True)
        with ro2: st.text_input("Period start date", value=start_dt, disabled=True)
        with ro3: st.text_input("Period end date", value=end_dt, disabled=True)
    else:
        ro1, ro2 = st.columns(2)
        start_dt = ro1.date_input("Period start date", value=None)
        end_dt = ro2.date_input("Period end date", value=None)
        next_start = None

    d1, d2, d3 = st.columns(3)
    processing_date = d1.date_input("Processing date*", value=None)
    n_emp = d2.number_input("Num employees processed*", min_value=1, step=1, value=None)
    n_states = d3.number_input("Num states processed*", min_value=1, step=1, value=None)

    help_this_period = False
    num_states_helped = 0
    if int(client.help_registration) == 1:
        help_choice = st.radio("Help this period?", options=["Yes", "No"], index=1, horizontal=True)
        help_this_period = (help_choice == "Yes")
        if help_this_period:
            num_states_helped = st.number_input("Num states helped*", min_value=1, step=1, value=None)

    a, b = st.columns([1,1])
    submit_clicked = a.button("Submit", type="primary", use_container_width=True)
    cancel_clicked = b.button("Cancel", use_container_width=True)

    if cancel_clicked:
        set_page("home_detail", id=client_id)
        st.rerun()

    if submit_clicked:
        errs = []
        if start_dt is None:
            errs.append("• Period Start Date is required.")
        else:
            ok, msg = validate_start_date(client.pay_frequency, start_dt)
            if not ok:
                errs.append(f"• {msg}")
        if end_dt is None:
            errs.append("• Period End Date is required.")
        elif start_dt is not None and period_type == "regular":
            expected_end = advance_pay_start_date(start_dt, client.pay_frequency) - timedelta(days=1)
            if end_dt != expected_end:
                errs.append(f"• Based on frequency and period start date, end date should be {expected_end}.")
        elif start_dt and period_type == "additional" and end_dt <= start_dt:
            errs.append("• Period end date should be after Period Start Date.")
        if processing_date is None or (start_dt is not None and processing_date < start_dt):
                errs.append("• Processing date must be on/after the Period Start Date.")
        if n_emp is None:
            errs.append("• Num employees processed must be > 0.")
        if n_states is None:
            errs.append("• Num states processed must be > 0.")
        if help_this_period and (num_states_helped is None or num_states_helped <= 0):
            errs.append("• Num states helped must be > 0 when help is selected.")

        if errs:
            st.error("Please fix the following:\n\n" + "\n\n".join(errs))
        else:
            snap = {
                "base_fee": float(client.base_fee),
                "additional_state_fee": float(client.additional_state_fee),
                "additional_employee_fee": float(client.additional_employee_fee),
                "num_states_in_base_fee": int(client.num_states_in_base_fee),
                "num_employees_in_base_fee": int(client.num_employees_in_base_fee),
                "help_registration": int(client.help_registration),
                "help_fee": float(client.help_fee),

                "num_states_processed": int(n_states),
                "num_employees_processed": int(n_emp),
                "help_this_period": help_this_period,
                "num_states_helped": int(num_states_helped or 0)
            }
            cost = calc_cost(**snap)
            new_period = {
                "client_id": int(client.id), "period_type": period_type, "pay_frequency": client.pay_frequency,
                "pay_start_date": start_dt.isoformat(), "pay_end_date": end_dt.isoformat(),
                "processing_date": processing_date.isoformat(), "cost": float(cost), **snap,
                "collected": 0.0, "collection_description": "", "collection_date": None, "net_amount": float(cost)
            }
            new_entry("transactions", new_period)

            if period_type == "regular":
                update_entry("clients", int(client_id), {"pay_start_date": next_start.isoformat()})
                st.session_state["reg_period"] = True
            else:
                st.session_state["add_period"] = True
            set_page("home_detail", id=client_id)
            st.rerun()

def render_edit_period(client_id):
    txn = fetch_row("transactions", txn_id)
    st.markdown(f"### Edit {txn.period_type.capitalize()} Pay Period")
    
    if txn.period_type == "regular":
        ro1, ro2, ro3 = st.columns(3)
        start_dt = date.fromisoformat(txn.pay_start_date)
        end_dt = date.fromisoformat(txn.pay_end_date)
        with ro1: st.text_input("Pay frequency", value=txn.pay_frequency, disabled=True)
        with ro2: st.text_input("Period start date", value=txn.pay_start_date, disabled=True)
        with ro3: st.text_input("Period end date", value=txn.pay_end_date, disabled=True)
    else:
        ro1, ro2 = st.columns(2)
        start_dt = ro1.date_input("Period start date*", value=date.fromisoformat(txn.pay_start_date))
        end_dt = ro2.date_input("Period end date*", value=date.fromisoformat(txn.pay_end_date))

    st.markdown("<br>", unsafe_allow_html=True)
    base_fee = st.number_input("Base Fee*", min_value=1.0, step=1.0, format="%.2f", value=txn.base_fee)
    g1, g2 = st.columns(2)
    num_states = g1.number_input("Number of States in Base Fee*", min_value=0, step=1, value=int(txn.num_states_in_base_fee))
    num_employees = g2.number_input("Number of Employees in Base Fee*", min_value=0, step=1, value=int(txn.num_employees_in_base_fee))
    st.markdown("<br>", unsafe_allow_html=True)
    f1, f2 = st.columns(2)
    add_state_fee = f1.number_input("Additional State Fee*", min_value=1.0, step=1.0, format="%.2f", value=txn.additional_state_fee)
    add_emp_fee = f2.number_input("Additional Employee Fee*", min_value=1.0, step=1.0, format="%.2f", value=txn.additional_employee_fee)

    e1, e2, e3 = st.columns(3)
    processing_date = e1.date_input("Processing date*", value=date.fromisoformat(txn.processing_date))
    n_emp = e2.number_input("Num employees processed*", min_value=1, step=1, value=int(txn.num_employees_processed))
    n_states = e3.number_input("Num states processed*", min_value=1, step=1, value=int(txn.num_states_processed))

    help_this_period = bool(int(txn.help_this_period))
    num_states_helped = int(txn.num_states_helped or 0)

    if int(txn.help_registration) == 1:
        help_choice = st.radio("Help this period?", options=["No", "Yes"],
                            index=1 if help_this_period else 0, horizontal=True)
        help_this_period = (help_choice == "Yes")
        if help_this_period:
            num_states_helped = st.number_input("Num states helped*", min_value=1, step=1,
                                                value=max(1, num_states_helped))
        else:
            num_states_helped = 0
    else:
        help_this_period = False
        num_states_helped = 0

    st.markdown("<br>", unsafe_allow_html=True)
    a, b = st.columns([1, 1])
    save_clicked = a.button("Save changes", type="primary", use_container_width=True)
    cancel_clicked = b.button("Cancel", use_container_width=True)

    if cancel_clicked:
        set_page("home_detail", id=client_id)
        st.rerun()

    if save_clicked:
        errs = []
        if start_dt is None:
            errs.append("• Period Start Date is required.")
        else:
            ok, msg = validate_start_date(txn.pay_frequency, start_dt)
            if not ok:
                errs.append(f"• {msg}")
        if end_dt is None:
            errs.append("• Period End Date is required.")
        elif start_dt is not None and end_dt <= start_dt:
            errs.append(f"• Period end date should be after Period Start Date.")
        if processing_date is None or (start_dt is not None and processing_date < start_dt):
                errs.append("• Processing date must be on/after the Period Start Date.")
        if n_emp is None:
            errs.append("• Num employees processed must be > 0.")
        if n_states is None:
            errs.append("• Num states processed must be > 0.")
        if help_this_period and (num_states_helped is None or num_states_helped <= 0):
            errs.append("• Num states helped must be > 0 when help is selected.")

        if errs:
            st.error("Please fix the following:\n\n" + "\n\n".join(errs))
        else:
            snap = {
                "base_fee": float(base_fee),
                "additional_state_fee": float(add_state_fee), 
                "additional_employee_fee": float(add_emp_fee),
                "num_states_in_base_fee": int(num_states), 
                "num_employees_in_base_fee": int(num_employees), 
                "num_states_processed": int(n_states),
                "num_employees_processed": int(n_emp),
                "help_this_period": help_this_period,
                "num_states_helped": int(num_states_helped or 0)
            }
            cost = calc_cost(**snap, help_registration=txn.help_registration, help_fee=txn.help_fee)
            update_entry("transactions", txn_id, {"pay_start_date": start_dt.isoformat(), "pay_end_date": end_dt.isoformat(),
                                                  "processing_date": processing_date.isoformat(), "cost": float(cost), **snap})
            st.session_state["edited_txn"] = True
            set_page("home_detail", id=client_id)
            st.rerun()

def fade_message():
    st.markdown(
        """
        <style>
        div[data-testid="stAlert"]{ animation: fadeout 0.5s ease 2.5s forwards; }
        @keyframes fadeout { to { opacity: 0;  max-height: 0; padding: 0; margin: 0; overflow: hidden; } }
        </style>
        """, unsafe_allow_html=True)
    
# ---- Modal: confirm terminate ----
@st.dialog("Confirm termination")
def confirm_terminate_dialog(client_id: int, client_name: str):
    st.write(f"Are you sure you want to terminate **{client_name}**?")

    c1, c2 = st.columns([1, 1])
    if c1.button("Yes, terminate", type="primary", key=f"term_yes_{client_id}"):
        update_entry("clients", client_id, {"terminated": 1})

        st.session_state.pop("terminate_open", None)
        st.session_state.pop("terminate_client_id", None)
        st.session_state["terminated_msg"] = True
        set_page("main")
        st.rerun()

    if c2.button("Cancel", key=f"term_no_{client_id}"):
        st.session_state.pop("terminate_open", None)
        st.session_state.pop("terminate_client_id", None)
        st.rerun()

# ---- Modal: fix effective date before reactivation ----
@st.dialog("Update fee increase effective date")
def reactivate_fix_dialog(client_id, client_name, current_eff):
    st.write(f"**{client_name}** must have a fee increase effective date afte today. Please change it continue.")
    new_eff = st.date_input("New effective date*", value=date.fromisoformat(current_eff))

    a, b = st.columns([1, 1])
    if a.button("Save & Reactivate", type="primary", key=f"react_save_{client_id}"):
        if new_eff <= date.today()  + timedelta(days=15):
            st.error("Effective date must be after today.")
        else:
            # Update the date and reactivate
            update_entry("clients", client_id, {"fee_increase_effective_date": new_eff.isoformat(), "terminated": 0})
            st.session_state["reactivated_msg"] = True
            set_page("admin_reactivate")
            st.rerun()

    if b.button("Cancel", key=f"react_cancel_{client_id}"):
        st.rerun()

@st.dialog("Cannot terminate client")
def cannot_terminate_dialog(client_name: str, net_amt: float):
    st.error(f"**{client_name}** cannot be terminated due to an outstanding balance of **${net_amt:,.2f}**.")
    st.caption(f"Ensure Accounts Receivable is 0 for {client_name} to be able to terminate them.")

# -------------- UI --------------
st.set_page_config(page_title="Clients Admin", page_icon="👥", layout="wide")
init_db()

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
.btn-click {
  display:inline-block;
  width:80px;              /* fixed width */
  text-align:center;
  padding:6px 0;           /* compact height */
  border-radius:8px;
  background:#3449ff;
  color:#ffffff !important;
  text-decoration:none !important;
}
.btn-click:hover { background:#2438e8; }
.txn-btn-cell { display:flex; justify-content:center; align-items:center; height:2rem; }
</style>
""", unsafe_allow_html=True)

# -------- Home Tab --------
with tabs[0]:
    home_pages = {"home_main", "home_detail", "home_edit", "new_period", "txn_edit"}
    page = current_page if current_page in home_pages else "home_main"

    if page == "home_main":
        if st.session_state.pop("terminated_msg", False):
            st.success("Client Terminated.")
            fade_message()

        st.subheader("Client List")

        display_cols = ["id", "client_name", "pay_frequency", "pay_start_date"]
        df = show_clients(display_cols, False)

        if df.empty:
            st.info("No active clients. Ask an Admin to assign one.")
        else:
            header = st.columns((0.5, 2, 2, 2), gap="small")
            header[0].markdown("<p style='text-align:center; font-weight:bold;'>Action</p>", unsafe_allow_html=True)
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Client Name</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Pay Frequency</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>Current Period Start Date</p>", unsafe_allow_html=True)
            
            for _, row in df.iterrows():
                cols = st.columns((0.5, 2, 2, 2), gap="small")

                # a, b = cols[0].columns(2, gap=None)
                # if a.button("View", key=f"view_{int(row['id'])}"):
                #     set_page("home_detail", id=int(row["id"]))
                #     st.rerun()
                # if b.button("✏️ Edit", key=f"edit_{int(row['id'])}"):
                #     set_page("home_edit", id=int(row["id"]))
                #     st.rerun()
                if cols[0].button("View", key=f"view_{int(row['id'])}", use_container_width=True):
                    set_page("home_detail", id=int(row["id"]))
                    st.rerun()

                cols[1].markdown(f"<p style='text-align:center;'>{row['client_name']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='text-align:center;'>{row['pay_frequency']}</p>", unsafe_allow_html=True)
                cols[3].markdown(f"<p style='text-align:center;'>{row['pay_start_date']}</p>", unsafe_allow_html=True)
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

        st.markdown(f"## {this_client.client_name} Details")

        c1, c2, c3, c4 = st.columns(4)
        if c1.button("Edit Client", use_container_width=True):
            set_page("home_edit", id=client_id)
            st.rerun()
        if c2.button("Regular pay period", use_container_width=True):
            set_page("new_period", id=client_id, type="regular")
            st.rerun()
        if c3.button("Additional pay period", use_container_width=True):
            set_page("new_period", id=client_id, type="additional")
            st.rerun()
        if c4.button("❌ Terminate", use_container_width=True):
            total_net = fetch_client_total_net_amount(client_id)
            if total_net > 0:
                st.session_state["show_cannot_terminate"] = True
                st.session_state["cannot_term_client_id"] = client_id
                st.rerun()
            else:
                st.session_state["terminate_client_id"] = client_id
                st.session_state["terminate_open"] = True
                st.rerun()
        
        if st.session_state.get("show_cannot_terminate") and st.session_state.get("cannot_term_client_id") == client_id:
            cannot_terminate_dialog(this_client.client_name, fetch_client_total_net_amount(client_id))
        if st.session_state.get("terminate_open") and st.session_state.get("terminate_client_id") == client_id:
            confirm_terminate_dialog(client_id, this_client.client_name)

        tx_df = fetch_client_transactions(client_id, "processing_date")
        if not tx_df.empty:
            st.markdown("<br><br>", unsafe_allow_html=True)
            st.markdown("### Previous Payments")

            header = st.columns((0.8, 2, 2, 2, 2, 2), gap="small")
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Period Type</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Start Date</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>End Date</p>", unsafe_allow_html=True)
            header[4].markdown("<p style='text-align:center; font-weight:bold;'>Processed Date</p>", unsafe_allow_html=True)
            header[5].markdown("<p style='text-align:center; font-weight:bold;'>Cost</p>", unsafe_allow_html=True)

            for _, row in tx_df.iterrows():
                cols = st.columns((0.8, 2, 2, 2, 2, 2), gap="small")

                # if cols[0].button("✏️Edit", key=f"tx_edit_{int(row['id'])}"):
                #     set_page("txn_edit", txn_id=int(row["id"]), id=client_id)
                #     st.rerun()
                with cols[0]:
                    st.markdown(
                        f"""
                        <div class="txn-btn-cell">
                            <a class="btn-click" href="?page=txn_edit&id={client_id}&txn_id={int(row['id'])}" target="_self">✏️ Edit</a>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                cols[1].markdown(f"<p style='text-align:center;'>{row['period_type']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='text-align:center;'>{row['pay_start_date']}</p>", unsafe_allow_html=True)
                cols[3].markdown(f"<p style='text-align:center;'>{row['pay_end_date']}</p>", unsafe_allow_html=True)
                cols[4].markdown(f"<p style='text-align:center;'>{row['processing_date']}</p>", unsafe_allow_html=True)
                cols[5].markdown(f"<p style='text-align:center;'>{"${:,.2f}".format(float(row["cost"]))}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)

    
    elif page == "home_edit":
        st.markdown("### Edit Client")
        left_pad, main, right_pad = st.columns([0.8, 2, 0.8])
        with main:
            raw = params.get("id")
            client_id = int(raw[0]) if isinstance(raw, list) else int(raw)
            row = fetch_row("clients", client_id)

            # Preload defaults into local vars (do not write to session_state to avoid key conflicts)
            name = st.text_input("Client Name*", value=row.client_name, key=f"edit_name_{client_id}")
            freq_options = ["weekly", "semi-weekly", "semi-monthly", "monthly"]

            pay_frequency = st.selectbox("Pay Frequency*", freq_options, index=freq_options.index(row.pay_frequency),
                                            key=f"edit_payfreq_{client_id}")
            pay_start_date = st.date_input("Period Start Date*", value=date.fromisoformat(row.pay_start_date),
                                            key=f"edit_start_{client_id}")
            base_fee = st.number_input("Base Fee*", min_value=1.0, step=1.0, format="%.2f", 
                                    value=float(row.base_fee), key=f"edit_base_{client_id}")
            num_states = st.number_input("Number of States in Base Fee*", min_value=0, step=1, 
                                        value=int(row.num_states_in_base_fee), key=f"edit_states_{client_id}")
            num_employees = st.number_input("Number of Employees in Base Fee*", min_value=0, step=1, 
                                            value=int(row.num_employees_in_base_fee), key=f"edit_emps_{client_id}")
            add_state_fee = st.number_input("Additional state fee*", min_value=1.0, step=1.0, format="%.2f",
                                            value=float(row.additional_state_fee), key=f"edit_addstate_{client_id}")
            add_emp_fee = st.number_input("Additional employee fee*", min_value=1.0, step=1.0, format="%.2f",
                                            value=float(row.additional_employee_fee), key=f"edit_addemp_{client_id}")
            
            method = st.radio("Fee Increase Method", options=["% Increase", "Manually Change Fee"], horizontal=True,
                            index=0 if row.use_pct_increase == 1 else 1, key=f"edit_method_{client_id}")
            if method == "% Increase":
                use_pct_increase = 1
                first, second = st.columns(2)
                inc_pct = first.number_input("Fee increase %*", min_value=1.0, step=1.0, format="%.2f",
                                        value=float(row.fee_increase_pct), key=f"edit_incpct_{client_id}")
                eff_date = second.date_input("Fee Increase Effective Date*", value=date.fromisoformat(row.fee_increase_effective_date), 
                                        key=f"edit_effdate_{client_id}")
                inc_base_override = float(row.increased_base_fee)  # not used when % method is selected
                inc_state_override = float(row.increased_additional_state_fee)
                inc_emp_override = float(row.increased_additional_employee_fee)
                
            else:
                use_pct_increase = 0
                inc_pct = float(row.fee_increase_pct)
                w, x, y, z = st.columns(4)
                inc_base_override = w.number_input("New base fee*", min_value=1.0, step=1.0, format="%.2f",
                    value=float(row.increased_base_fee), key=f"edit_incbase_{client_id}")
                inc_state_override = x.number_input("New additional state fee*", min_value=1.0, step=1.0, format="%.2f",
                    value=float(row.increased_additional_state_fee), key=f"edit_incstate_{client_id}")
                inc_emp_override = y.number_input("New additional employee fee*", min_value=1.0, step=1.0, format="%.2f",
                    value=float(row.increased_additional_employee_fee), key=f"edit_incemps_{client_id}")
                eff_date = z.date_input("Fee Increase Effective Date*", value=date.fromisoformat(row.fee_increase_effective_date), 
                                        key=f"edit_effdate_{client_id}")
            
            help_choice = st.radio("Help registration?", options=["Yes", "No"], index=0 if row.help_registration == 1 else 1,
                                    horizontal=True, key=f"edit_helpreg_{client_id}")
            if help_choice == "Yes": 
                help_fee = st.number_input("Help Fee*", min_value=0.0, step=1.0, format="%.2f",
                                                value=float(row.help_fee), key=f"edit_helpfee_{client_id}")
            else:
                help_fee = 0.0
            
            a,b = st.columns([1, 1])
            save_clicked = a.button("💾 Save changes", type="primary", key=f"edit_save_{client_id}", use_container_width=True)
            cancel_clicked = b.button("Cancel", key=f"edit_cancel_{client_id}", use_container_width=True)

            if cancel_clicked:
                set_page("home_detail", id=client_id)
                st.rerun()
            if save_clicked:
                errors = []
                if not name.strip():
                    errors.append("• Client Name is required.")
                if pay_start_date is None:
                    errors.append("• Pay start date is required.")
                latest_end = fetch_latest_pay_end_date(client_id)
                if latest_end is not None and pay_start_date <= latest_end:
                    errors.append(f"• Pay start date must be after the latest pay end date ({latest_end.isoformat()}).")
                ok, msg = validate_start_date(pay_frequency, pay_start_date)
                if not ok:
                    errors.append(f"• {msg}")
                if eff_date is None:
                    errors.append("• Fee increase effective date is required.")
                elif eff_date <= date.today():
                    errors.append("• Fee increase effective date must be after today's date.")

                help_reg = (help_choice == "Yes")
                if help_reg and help_fee <= 0:
                    errors.append("• Help Fee must be > 0.")
                
                if errors:
                    st.error("Please fix the following before saving:\n\n" + "\n\n".join(errors))
                else:
                    client_info = {
                        "client_name": name.strip(), "pay_frequency": pay_frequency, "pay_start_date": pay_start_date.isoformat(),
                        "base_fee": float(base_fee), "num_states_in_base_fee": int(num_states), 
                        "num_employees_in_base_fee": int(num_employees), "additional_state_fee": float(add_state_fee), 
                        "additional_employee_fee": float(add_emp_fee), "fee_increase_pct": float(inc_pct), 
                        "help_registration": int(help_reg), "help_fee": float(help_fee), "use_pct_increase": int(use_pct_increase)
                    }

                    if use_pct_increase == 1:
                        client_info["fee_increase_effective_date"] = eff_date.isoformat()
                        client_info["increased_base_fee"] = pct_increase(base_fee, inc_pct)
                        client_info["increased_additional_state_fee"] = pct_increase(add_state_fee, inc_pct)
                        client_info["increased_additional_employee_fee"] = pct_increase(add_emp_fee, inc_pct)
                    else:
                        client_info["fee_increase_effective_date"] = eff_date.isoformat()
                        client_info["increased_base_fee"] = round(float(inc_base_override), 2)
                        client_info["increased_additional_state_fee"] = round(float(inc_state_override), 2)
                        client_info["increased_additional_employee_fee"] = round(float(inc_emp_override), 2)

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
        
        render_edit_period(client_id)


# ========== ADMIN TAB ==========
with tabs[1]:
    admin_pages = {"admin_add", "admin_reactivate", "admin_collect", "admin_reports"}
    page = current_page if current_page in admin_pages else "admin_main"

    if page == "admin_main":
        st.subheader("Admin")
        if st.session_state.pop("new_client", False):
            st.success("Client created successfully.")
            fade_message()
        if st.session_state.pop("collections_saved", False):
            st.success("Collection details updated.")
            fade_message()
        a, b, c = st.columns(3)
        if a.button("➕ Add new client", use_container_width=True):
            set_page("admin_add")
            st.rerun()
        if b.button("♻️ Reactivate Clients", use_container_width=True):
            set_page("admin_reactivate")
            st.rerun()
        if c.button("📄 Generate Reports", use_container_width=True):
            set_page("admin_reports")
            st.rerun()

        st.markdown("---")
        st.markdown("### Collections Overview")

        agg = fetch_admin_aggregates()
        if not agg.empty:
            header = st.columns((1, 2, 2, 2, 2), gap="small")
            header[0].markdown("<p style='text-align:center; font-weight:bold;'>Action</p>", unsafe_allow_html=True)
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Client Name</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Total Cost</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>Total Collected</p>", unsafe_allow_html=True)
            header[4].markdown("<p style='text-align:center; font-weight:bold;'>Amounts Receivable</p>", unsafe_allow_html=True)

            for _, r in agg.iterrows():
                cols = st.columns((1, 2, 2, 2, 2), gap="small")

                if cols[0].button("Manage", key=f"collection_{int(r['client_id'])}"):
                    set_page("admin_collect", id=int(r["client_id"]))
                    st.rerun()

                cols[1].markdown(f"<p style='text-align:center;'>{r['client_name']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='text-align:center;'>${float(r['cost']):,.2f}</p>", unsafe_allow_html=True)
                cols[3].markdown(f"<p style='text-align:center;'>${float(r['total_collected']):,.2f}</p>", unsafe_allow_html=True)
                cols[4].markdown(f"<p style='text-align:center;'>${float(r['net_amount']):,.2f}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)

            
    elif page == "admin_add":
        st.markdown("### New Client")

        name = st.text_input("Client Name*",key="client_name", value=None)

        freq_options = ["weekly", "semi-weekly", "semi-monthly", "monthly"]
        pay_frequency = st.selectbox("Pay frequency*", freq_options, key="pay_frequency",
                     index=0)

        pay_start_date = st.date_input("Pay start date*", value=None)
        base_fee = st.number_input("Base fee*", min_value=1.0, step=1.0, format="%.2f", value=None)
        num_states = st.number_input("Number of states in base fee*", min_value=0, step=1, value=None)
        num_employees = st.number_input("Number of employees in base fee*",  min_value=0, step=1, value=None)
        add_state_fee = st.number_input("Additional state fee*", min_value=1.0, step=1.0, format="%.2f", value=None)
        add_emp_fee = st.number_input("Additional employee fee*", min_value=1.0, step=2.0, format="%.2f", value=None)
        inc_pct = st.number_input("Fee increase %*", min_value=1.0, step=1.0, format="%.2f", value=None)

        help_choice = st.radio("Help registration?", options=["Yes", "No"], horizontal=True, index=1)
        if help_choice == "Yes":
            help_fee = st.number_input("Help Fee*", min_value=0.0, step=1.0, format="%.2f", value=None)
        else:
            help_fee = 0.00

        a_col, b_col = st.columns([1, 1])
        create_clicked = a_col.button("Create Client", type="primary", use_container_width=True)
        cancel_clicked = b_col.button("Cancel", use_container_width=True)

        if cancel_clicked:
            set_page("admin_main")
            st.rerun()

        if create_clicked:
            errors = []
            if not name:
                errors.append("• Client Name is required.")
            if pay_start_date is None:
                errors.append("• Pay start date is required.")
            else:
                ok, msg = validate_start_date(pay_frequency, pay_start_date)
                if not ok:
                    errors.append(f"• {msg}")
            
            if base_fee is None: errors.append("• Base fee must be a number > 0.")
            if num_states is None: errors.append("• Number of states in base fee must be a number >= 0.")
            if num_employees is None: errors.append("• Number of employees in base fee must be a number >= 0.")
            if add_state_fee is None: errors.append("• Additional state fee must be a number > 0.")
            if add_emp_fee is None: errors.append("• Additional employee fee must be a number > 0.")
            if inc_pct is None: errors.append("• Fee increase % must be a number > 0.")

            help_reg = (help_choice == "Yes")
            if help_reg:
                if help_fee <= 0: errors.append("• Help fee must be > 0.")
            
            if errors:
                st.error("Please fix the following before creating a new client:\n\n" + "\n\n".join(errors))
            else:
                new_client = {
                    "client_name": name,
                    "pay_frequency": pay_frequency,
                    "pay_start_date": pay_start_date.isoformat(),
                    "base_fee": base_fee,
                    "num_states_in_base_fee": num_states,
                    "num_employees_in_base_fee": num_employees,
                    "additional_state_fee": add_state_fee,
                    "additional_employee_fee": add_emp_fee,
                    "fee_increase_pct": inc_pct,
                    "use_pct_increase": 1,
                    "help_registration": int(help_reg),
                    "help_fee": help_fee,
                    "fee_increase_effective_date": plus_one_year(pay_start_date).isoformat(),
                    "increased_base_fee": pct_increase(base_fee, inc_pct),
                    "increased_additional_state_fee": pct_increase(add_state_fee, inc_pct),
                    "increased_additional_employee_fee": pct_increase(add_emp_fee, inc_pct)
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

        display_cols = ["id", "client_name", "pay_frequency", "pay_start_date", "fee_increase_effective_date"]
        df = show_clients(display_cols, True)

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

            header = st.columns((1, 2, 2, 2), gap="small")
            header[0].markdown("<p style='text-align:center; font-weight:bold;'>Action</p>", unsafe_allow_html=True)
            header[1].markdown("<p style='text-align:center; font-weight:bold;'>Client Name</p>", unsafe_allow_html=True)
            header[2].markdown("<p style='text-align:center; font-weight:bold;'>Pay Frequency</p>", unsafe_allow_html=True)
            header[3].markdown("<p style='text-align:center; font-weight:bold;'>Current Period Start Date</p>", unsafe_allow_html=True)

            for _, row in df.iterrows():
                cols = st.columns((1, 2, 2, 2), gap="small")
                
                if cols[0].button("Reactivate", key=f"react_{int(row['id'])}"):
                    if date.fromisoformat(row["fee_increase_effective_date"]) < date.today() + timedelta(days=15):
                        reactivate_fix_dialog(int(row["id"]), row['client_name'], row["fee_increase_effective_date"])
                    else:
                        update_entry("clients", int(row["id"]), {"terminated": 0})
                        st.session_state["reactivated_msg"] = True
                        st.rerun()

                cols[1].markdown(f"<p style='text-align:center;'>{row['client_name']}</p>", unsafe_allow_html=True)
                cols[2].markdown(f"<p style='text-align:center;'>{row['pay_frequency']}</p>", unsafe_allow_html=True)
                cols[3].markdown(f"<p style='text-align:center;'>{row['pay_start_date']}</p>", unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.0rem 0;'>", unsafe_allow_html=True)
    
    elif page == "admin_collect":
        raw = params.get("id")
        client_id = int(raw[0]) if isinstance(raw, list) else int(raw)
        client = fetch_row("clients", client_id)

        st.markdown(f"### Collections for {client.client_name}")

        df = fetch_transactions_for_admin_edit(client_id)
        if df.empty:
            if st.button("← Back"):
                set_page("admin_main")
                st.rerun()
            st.info(f"{client.client_name} does not have any money to be collected.")
        else:
            # Convert dates to proper types for display/edit
            for col in ["pay_start_date", "pay_end_date", "collection_date"]:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

            # Visible & order
            display_cols = ["id", "pay_start_date", "pay_end_date", "cost", "collected",
                            "collection_description", "collection_date", "net_amount"]
            df = df[display_cols]

            df["net_amount"] = (df["cost"].astype(float) - df["collected"].fillna(0).astype(float)).round(2)

            # Show an editable grid for the three fields only
            edited = st.data_editor(
                df, use_container_width=True, hide_index=True,
                column_config={
                    "id": st.column_config.Column("ID", disabled=True, width="small"),
                    "pay_start_date": st.column_config.DateColumn("Pay start date", disabled=True),
                    "pay_end_date": st.column_config.DateColumn("Pay end date", disabled=True),
                    "cost": st.column_config.NumberColumn("Cost", disabled=True, format="%.2f"),
                    "collected": st.column_config.NumberColumn("Collected", min_value=0.0, step=1.0, format="%.2f", default=None),
                    "collection_description": st.column_config.TextColumn("Collection description"),
                    "collection_date": st.column_config.DateColumn("Collection date"),
                    "net_amount": st.column_config.NumberColumn("Net amount", disabled=True, format="%.2f"),
                },
                key=f"admin_collect_editor_{client_id}",
            )

            # Live recompute net in the grid view (visual), final recompute on save
            # if not edited.empty:
            #     edited["net_amount"] = (edited["cost"].astype(float) - edited["collected"].fillna(0).astype(float)).round(2)

            a, b = st.columns([1,1])
            save_clicked = a.button("Save changes", type="primary", use_container_width=True)
            cancel_clicked = b.button("Cancel", use_container_width=True)

            if cancel_clicked:
                set_page("main")
                st.rerun()

            if save_clicked:
                update_transaction_collection_fields(edited)
                st.session_state["collections_saved"] = True
                set_page("admin_main")  # back to Admin main; aggregates will reflect updates
                st.rerun()
    
    elif page == "admin_reports":
        st.markdown("### Reports")
        st.caption("Download CSV exports for fee increases and collections overview.")

        st.markdown("#### 1) Clients with previous fee increase effective dates")
        col1, col2 = st.columns([1, 4])
        with col1:
            df_prev = df_clients_fee_increase_past()
            csv_prev = df_prev.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_prev,
                file_name=f"clients_previous_fee_increase_{date.today().isoformat()}.csv",
                mime="text/csv",
                key="dl_prev_fee_increase",
            )
        with col2:
            # optional tiny preview
            if not df_prev.empty:
                st.dataframe(df_prev, use_container_width=True, hide_index=True, height=220)

        st.markdown("---")

        st.markdown("#### 2) Clients with future fee increase effective dates")
        col3, col4 = st.columns([1, 4])
        with col3:
            df_fut = df_clients_fee_increase_future()
            csv_fut = df_fut.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_fut,
                file_name=f"clients_future_fee_increase_{date.today().isoformat()}.csv",
                mime="text/csv",
                key="dl_future_fee_increase",
            )
        with col4:
            if not df_fut.empty:
                st.dataframe(df_fut, use_container_width=True, hide_index=True, height=220)

        st.markdown("---")

        st.markdown("#### 3) Collections overview (by processing date range)")
        # date range inputs
        f1, f2, f3 = st.columns([1.5, 1.5, 1])
        with f1:
            range_start = st.date_input("Range start*", value=None, key="rep_range_start")
        with f2:
            range_end = st.date_input("Range end*", value=None, key="rep_range_end")
        with f3:
            gen_clicked = st.button("Generate CSV", type="primary", key="rep_generate")

        # validate & generate
        if gen_clicked:
            errs = []
            if range_start is None:
                errs.append("• Start date is required.")
            if range_end is None:
                errs.append("• End date is required.")
            if range_start and range_end and range_end < range_start:
                errs.append("• End date must be on/after start date.")

            if errs:
                st.error("Please fix the following:\n\n" + "\n\n".join(errs))
            else:
                df_rng = df_collections_overview_range(range_start, range_end)
                csv_rng = df_rng.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="⬇️ Download CSV (by range)",
                    data=csv_rng,
                    file_name=f"collections_overview_{range_start.isoformat()}_to_{range_end.isoformat()}.csv",
                    mime="text/csv",
                    key="dl_collections_range",
                )
                if not df_rng.empty:
                    st.dataframe(df_rng, use_container_width=True, hide_index=True)

        st.markdown("---")
        if st.button("← Back"):
            set_page("main")
            st.rerun()