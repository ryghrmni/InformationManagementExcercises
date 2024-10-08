import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Create main window
root = tk.Tk()
root.title('ETL Pipeline with Tkinter')
root.geometry('500x400')

# Global variables to store file path and transformed data
file_path = ''
transformed_data = None

# Function to upload CSV file
def upload_file():
    global file_path
    file_path = filedialog.askopenfilename(
        filetypes=[("CSV files", "*.csv")], title="Choose a CSV file"
    )
    if file_path:
        messagebox.showinfo("File Selected", f"File: {file_path}")
    else:
        messagebox.showwarning("No file", "Please select a file")

# Function to transform the data
def transform_data():
    global transformed_data, file_path
    if not file_path:
        messagebox.showwarning("No file", "Please upload a CSV file first")
        return
    
    # Extract and transform data
    data = pd.read_csv(file_path)
    data['Value'] = pd.to_numeric(data['Value'], errors='coerce')
    cleaned_data = data.dropna(subset=['Value', 'Variable_name'])
    total_income = cleaned_data[cleaned_data['Variable_name'] == 'Total income']
    transformed_data = total_income.groupby(['Year', 'Industry_name_NZSIOC']).agg({'Value': 'sum'}).reset_index()

    # Show a success message
    messagebox.showinfo("Transformation Successful", "Data has been transformed.")

# Function to load data into SQLite
def load_data():
    global transformed_data
    if transformed_data is None:
        messagebox.showwarning("No data", "Please transform the data first")
        return
    
    # Load data into SQLite
    engine = create_engine('sqlite:///etl_database.db')
    transformed_data.to_sql('total_income_summary', con=engine, if_exists='replace', index=False)
    
    messagebox.showinfo("Load Successful", "Data has been loaded into SQLite database.")

# Function to visualize data
def visualize_data():
    global transformed_data
    if transformed_data is None:
        messagebox.showwarning("No data", "Please transform the data first")
        return
    
    # Visualize top 5 industries by total income
    top_5_industries = transformed_data.sort_values(by='Value', ascending=False).head(5)
    
    plt.figure(figsize=(10, 6))
    plt.bar(top_5_industries['Industry_name_NZSIOC'], top_5_industries['Value'], color='skyblue')
    plt.title('Top 5 Industries by Total Income in 2023')
    plt.xlabel('Industry')
    plt.ylabel('Total Income (Millions)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# Create GUI buttons
upload_btn = tk.Button(root, text="Upload CSV", command=upload_file, width=25, height=2)
transform_btn = tk.Button(root, text="Transform Data", command=transform_data, width=25, height=2)
load_btn = tk.Button(root, text="Load Data to SQLite", command=load_data, width=25, height=2)
visualize_btn = tk.Button(root, text="Visualize Data", command=visualize_data, width=25, height=2)

# Place buttons on the window
upload_btn.pack(pady=20)
transform_btn.pack(pady=10)
load_btn.pack(pady=10)
visualize_btn.pack(pady=10)

# Run the Tkinter loop
root.mainloop()