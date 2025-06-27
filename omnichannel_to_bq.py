import tkinter as tk
from tkinter import messagebox, simpledialog
from tkinter import ttk
from tkcalendar import DateEntry
from datetime import datetime
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import bigquery

dotenv_path = os.path.join(os.path.dirname(__file__), "information.env")
load_dotenv(dotenv_path)

def get_iso_from_input(date_widget, hour_widget, minute_widget, second_widget):
    try:
        date_str = date_widget.get()
        hour_str = hour_widget.get()
        min_str = minute_widget.get()
        sec_str = second_widget.get()
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        h, m, s = int(hour_str), int(min_str), int(sec_str)
        dt = dt.replace(hour=h, minute=m, second=s)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

class CaresoftAPIClient:
    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    def fetch_data(self, created_since, created_to, obj_key='deals', count=500, max_retries=10, timeout=600):
        all_objs, page = [], 1
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        while True:
            data = None
            retries = 0
            while retries < max_retries:
                try:
                    request_url = f"{self.api_url}?created_since={created_since}&created_to={created_to}&count={count}&page={page}"
                    print(f"[API] {request_url} - Thử {retries+1}/{max_retries}")
                    resp = requests.get(request_url, headers=headers, timeout=timeout)
                    resp.raise_for_status()
                    data = resp.json().get(obj_key, [])
                    if not data:
                        print(f"Hết dữ liệu từ API {request_url}")
                        return pd.DataFrame(all_objs)
                    all_objs.extend(data)
                    page += 1
                    break
                except Exception as e:
                    print(f"[API] Lỗi: {e}")
                    retries += 1
            if data is None:
                print(f"Không thể lấy dữ liệu sau {max_retries} lần thử cho page {page}")
                break
            if len(data) < count:
                print("Đã lấy hết toàn bộ page.")
                break
        return pd.DataFrame(all_objs)

class DataFrameProcessor:
    @staticmethod
    def cast_columns(df):
        # Khai báo danh sách các cột cần ép kiểu
        int_cols_list = [
        'id', 'deal_no', 'requester_id', 'lead_id', 'customer_id', 'user_id', 
        'count', 'so_luong', 'amount', 'qty', 'is_active', 'is_overdue'
            ]   
        date_cols_list = [
        'created_at', 'updated_at', 'created_time', 'updated_time', 
        'start_date', 'end_date', 'date', 'timestamp', 'closed_at'
            ]
        int_cols, date_cols, string_cols = [], [], []

        for col in df.columns:
            col_lower = col.lower()
            # INT
            if col_lower in int_cols_list:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    int_cols.append(col)
                except Exception as e:
                    print(f"Không ép được INT: {col}, lỗi: {e}")
            # DATE/TIMESTAMP
            elif col_lower in date_cols_list:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                    date_cols.append(col)
                except Exception as e:
                    print(f"Không ép được TIMESTAMP: {col}, lỗi: {e}")
            # STRING
            else:
                df[col] = df[col].astype(str).fillna("")
                string_cols.append(col)

        if int_cols:
            print(f"Ép INT: {', '.join(int_cols)}")
        if date_cols:
            print(f"Ép TIMESTAMP: {', '.join(date_cols)}")
        if string_cols:
            print(f"Ép STRING: {', '.join(string_cols)}")
        return df

    @staticmethod
    def sort_by_created_at(df):
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df = df.sort_values('created_at', ascending=True, ignore_index=True)
            df['created_at'] = df['created_at'].dt.strftime("%Y-%m-%d %H:%M:%S")
            print("Đã sort ASC theo created_at.")
        else:
            print("Không có cột created_at để sort.")
        return df

    @staticmethod
    def show_dataframe_head(df):
        print("5 dòng đầu DataFrame sau khi chuẩn hóa & sort:")
        print(df.head())
        print("Các cột:", list(df.columns))
        print(f"Số dòng: {df.shape[0]}")

class BigQueryManager:
    def __init__(self):
        self.keyfile = os.getenv("GCP_KEYFILE")
        self.project = os.getenv("GCP_PROJECT")
        self.bq_dataset = os.getenv("BQ_DATASET")
        self.client = bigquery.Client.from_service_account_json(self.keyfile, project=self.project)

    def list_tables(self):
        try:
            tables = list(self.client.list_tables(self.bq_dataset))
            table_names = [tbl.table_id for tbl in tables]
            print(f"Đã kết nối BigQuery project: {self.project}, dataset: {self.bq_dataset}")
            print("Các bảng hiện có:", table_names)
            return table_names
        except Exception as e:
            messagebox.showerror("Lỗi", f"Kết nối BigQuery thất bại: {e}")
            return []

    def import_to_table(self, df, table_name):
        table_id = f"{self.project}.{self.bq_dataset}.{table_name}"
        temp_csv = "___temp_for_bq.csv"
        df.to_csv(temp_csv, index=False, encoding="utf-8")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
        try:
            with open(temp_csv, "rb") as f:
                job = self.client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()
            os.remove(temp_csv)
            print(f"Đã import df vào BigQuery {table_id}")
            total = list(self.client.query(f"SELECT COUNT(*) as cnt FROM `{table_id}`").result())[0].cnt
            print(f"Số dòng staging: {total}")
            return True
        except Exception as e:
            os.remove(temp_csv)
            messagebox.showerror("Lỗi", f"Import vào staging thất bại: {e}")
            return False

    def merge_tables(self, staging_table, main_table, key_field="id"):
        try:
            table_ref = f"{self.project}.{self.bq_dataset}.{staging_table}"
            schema = self.client.get_table(table_ref).schema
            all_cols = [field.name for field in schema]
            update_cols = [col for col in all_cols if col != key_field]
            set_clause = ", ".join([f"{col} = S.{col}" for col in update_cols])
            insert_cols = ", ".join(all_cols)
            insert_vals = ", ".join([f"S.{col}" for col in all_cols])
            merge_sql = f"""
            MERGE `{self.project}.{self.bq_dataset}.{main_table}` T
            USING `{self.project}.{self.bq_dataset}.{staging_table}` S
            ON T.{key_field} = S.{key_field}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            print("Merge SQL:\n", merge_sql)
            job = self.client.query(merge_sql)
            job.result()
            total = list(self.client.query(f"SELECT COUNT(*) as cnt FROM `{self.project}.{self.bq_dataset}.{main_table}`").result())[0].cnt
            print(f"Đã MERGE vào {main_table}. Số dòng: {total}")
            return True
        except Exception as e:
            messagebox.showerror("Lỗi", f"Merge vào bảng chính thất bại: {e}")
            return False

    def drop_table(self, table_name):
        table_id = f"{self.project}.{self.bq_dataset}.{table_name}"
        try:
            self.client.delete_table(table_id, not_found_ok=True)
            print(f"Đã xóa bảng {table_id}")
            return True
        except Exception as e:
            print(f"Lỗi xóa bảng {table_id}: {e}")
            return False
        
class Caresoft2BigQueryApp:
    def __init__(self, api_url, api_obj_key="deals"):
        self.api_url = api_url
        self.api_obj_key = api_obj_key
        self.api_key = os.getenv("API_KEY")
        self.bq = BigQueryManager()
        self.processor = DataFrameProcessor()

    def get_datetime_iso_gui(self, label, master, row_idx):
        tk.Label(master, text=label).grid(row=row_idx, column=0)
        date = DateEntry(master, width=10, date_pattern="yyyy-mm-dd")
        date.grid(row=row_idx, column=1)
        tk.Label(master, text="Giờ:").grid(row=row_idx, column=2)
        hour = tk.Spinbox(master, from_=0, to=23, width=3, format="%02.0f")
        hour.grid(row=row_idx, column=3)
        tk.Label(master, text="Phút:").grid(row=row_idx, column=4)
        minute = tk.Spinbox(master, from_=0, to=59, width=3, format="%02.0f")
        minute.grid(row=row_idx, column=5)
        tk.Label(master, text="Giây:").grid(row=row_idx, column=6)
        second = tk.Spinbox(master, from_=0, to=59, width=3, format="%02.0f")
        second.grid(row=row_idx, column=7)
        return date, hour, minute, second

    def main_gui(self):
        root = tk.Tk()
        root.title("Get Data")

        since_widget = self.get_datetime_iso_gui("Ngày bắt đầu (since):", root, 0)
        to_widget = self.get_datetime_iso_gui("Ngày kết thúc (to):", root, 1)

        def on_submit():
            try:
                created_since = get_iso_from_input(*since_widget)
                created_to = get_iso_from_input(*to_widget)
                if not created_since or not created_to:
                    messagebox.showerror("Lỗi", "Nhập sai định dạng ngày/giờ!")
                    root.destroy(); return

                api = CaresoftAPIClient(self.api_url, self.api_key)
                df = api.fetch_data(created_since, created_to, obj_key=self.api_obj_key)
                if df.empty:
                    messagebox.showerror("Lỗi", "Không lấy được dữ liệu từ API!"); root.destroy(); return

                df = self.processor.cast_columns(df)
                df = self.processor.sort_by_created_at(df)

                self.processor.show_dataframe_head(df)

                table_names = self.bq.list_tables()

                if not self.bq.client:
                    root.destroy(); return

                action_win = tk.Toplevel(root)
                action_win.title("Chọn thao tác")
                tk.Label(action_win, text="Chọn thao tác:").pack(pady=5)
                action_var = tk.StringVar(action_win)
                action_var.set("new")
                combo = ttk.Combobox(action_win, textvariable=action_var, values=["new", "update"], state="readonly")
                combo.pack(padx=10, pady=5)

                def on_action_ok():
                    action_win.destroy()
                action_btn = tk.Button(action_win, text="OK", command=on_action_ok)
                action_btn.pack(pady=5)
                action_win.grab_set()
                root.wait_window(action_win)
                action = action_var.get()

                if action == "new":
                    table_new = simpledialog.askstring("Tạo bảng mới", "Nhập tên bảng mới muốn tạo:")
                    if not table_new:
                        messagebox.showinfo("Kết thúc", "Không nhập tên bảng, thoát."); root.destroy(); return
                    ok = self.bq.import_to_table(df, table_new.strip())
                    if ok:
                        messagebox.showinfo("Thành công", f"Đã tạo bảng mới và import xong: {table_new.strip()}")

                elif action == "update":
                    if not table_names:
                        messagebox.showerror("Lỗi", "Dataset chưa có bảng nào."); root.destroy(); return
                    update_win = tk.Toplevel(root)
                    update_win.title("Chọn bảng update")
                    tk.Label(update_win, text="Chọn bảng để cập nhật:").pack()
                    sel = tk.StringVar(update_win); sel.set(table_names[0])
                    combo = ttk.Combobox(update_win, textvariable=sel, values=table_names)
                    combo.pack()

                    def on_update_submit():
                        main_table = sel.get()
                        staging_table = simpledialog.askstring("Tên bảng staging", "Nhập tên bảng staging (ví dụ: staging_deal_test):")
                        if not staging_table:
                            messagebox.showerror("Lỗi", "Chưa nhập tên bảng staging.")
                            update_win.destroy(); root.destroy(); return
                        ok1 = self.bq.import_to_table(df, staging_table.strip())
                        if not ok1:
                            update_win.destroy(); root.destroy(); return
                        ok2 = self.bq.merge_tables(staging_table.strip(), main_table.strip(), key_field="id")
                        if ok2:
                            self.bq.drop_table(staging_table.strip())
                            messagebox.showinfo("Thành công", f"Đã merge và xóa bảng staging: {staging_table.strip()}\nBảng chính: {main_table.strip()}")
                            update_win.destroy(); root.destroy()
                    tk.Button(update_win, text="Cập nhật", command=on_update_submit).pack()
                else:
                    messagebox.showinfo("Kết thúc", "Chưa hỗ trợ thao tác này.")
                    root.destroy(); return
            except Exception as e:
                messagebox.showerror("Lỗi tổng", str(e))
                root.destroy()
        btn = tk.Button(root, text="Lấy API & Đẩy BigQuery", command=on_submit)
        btn.grid(row=2, column=0, columnspan=8, pady=10)
        root.mainloop()

if __name__ == "__main__":
    app = Caresoft2BigQueryApp(api_url="https://api.caresoft.vn/bvphuongdong/api/v1/deals", api_obj_key="deals")
    app.main_gui()
