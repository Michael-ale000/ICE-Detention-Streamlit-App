import streamlit as st
import pandas as pd
import os
import tempfile
import sys
from io import BytesIO
# Make local modules importable
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from TitleAndTableExtraction import Extraction_title_and_data
from Validation import validation
from Transformation import transformation
from Final_System import sanitize, store_in_db1  
from visualization import Visualization
from io import BytesIO
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import snowflake.connector
import io
def send_email_with_excel(sender_email, sender_password, recipient_email, subject,
                          df_dict: dict, filename="ICE_Tables.xlsx"):
    try:
        # Create email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText("Attached is the Excel file with multiple sheets of ICE data.", 'plain'))

        # Save DataFrames to Excel in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet_name, df in df_dict.items():
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # sheet names must be <= 31 chars
        output.seek(0)

        # Attach Excel file
        attachment = MIMEApplication(output.read(), _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(attachment)

        # Send email
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()

        return True, "Email with Excel file sent successfully!"
    except Exception as e:
        return False, f"Error occurred: {e}"

# 1.  Streamlit page config
st.set_page_config(page_title="Detention Data Uploader", layout="wide")
st.title("📊 ICE DETENTION SYSTEM")
st.write("Upload an Excel file to extract, validate, transform, and store detention data.")

# 2.  Ask the user for their API key (masked)
api_key = st.text_input(
    "🔑 Enter your OpenAI (or other) API key",
    type="password",
    help="Your key stays only in this session and is **not** logged."
)
username = st.text_input("Snowflake Username")
password = st.text_input("Snowflake Password", type="password")
schema = st.text_input("Snowflake Schema", value="DETENTIONS")
# 3.  File uploader
uploaded_file = st.file_uploader("📂 Upload Excel file", type=["xlsx"])

# 4.  Main pipeline

if uploaded_file is not None:
    if not api_key:
        st.warning("Please provide a valid API key before processing the file.")
        st.stop()

    # Save upload to a temp file so downstream functions can read it
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(uploaded_file.getvalue())
        temp_file_path = tmp.name

    st.success("File uploaded successfully!")

    try:
        st.info("🔍 Extracting tables…")
        dfs = Extraction_title_and_data(
            temp_file_path,
            source_filename=uploaded_file.name,
            api_key=api_key        # ← pass it through
        )

        # Convert keys to “Table 1”, “Table 2”, …
        tables = {f"Table {i}": df for i, df in enumerate(dfs.values(), start=1)}

  
        st.info("✅ Validating tables…")
        validated_tables, report = validation(tables)

       
        st.info("🔧 Transforming tables…")
        transformed_tables = transformation(validated_tables)

       
        st.success("✅ Tables processed successfully. Preview below:")
        selected_table = st.selectbox("Select a table to preview", list(transformed_tables.keys()))
        st.dataframe(transformed_tables[selected_table])
        # -------Tables that need to be send to Austin and Adam ---------
        df1 = transformed_tables["Table 1"]
        df4 = transformed_tables["Table 4"]
        df5 = transformed_tables["Table 5"]
        df6 = transformed_tables["Table 6"]
        df8 = transformed_tables["Table 8"]
        df_dict = {
            "Table 1": df1,
            "Table 4": df4,
            "Table 5": df5,
            "Table 6": df6,
            "Table 8": df8
        }
        if st.button("📤 Upload to Snowflake"):
            store_in_db1(transformed_tables,username, password,schema)
            st.success("Data uploaded to Snowflake successfully!")
        sender_email = st.text_input("Your Email Address")
        sender_password = st.text_input("Your Email Password / App Password", type="password")
        recipient_email = st.text_input("Recipient Email")
        subject = st.text_input("Subject", value="Table data from ICE Detention System")
        if st.button("Send Email"):
            sender_email = sender_email
            sender_password = sender_password
            recipient_email = recipient_email
            subject = subject
            message_body = df1.to_csv(index=False)
            if sender_email and sender_password and recipient_email and subject and message_body:
                success,msg = send_email_with_excel(sender_email, sender_password, recipient_email, subject, df_dict)
                if success:
                    st.success("Email sent successfully!")
                else:
                    st.error(msg)
            else:
                st.warning("Please fill out all fields.")        
    except Exception as e:
        st.error(f"❌ Error occurred: {e}")
def fig_to_bytes(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches='tight')
    buf.seek(0)
    return buf


if st.button("Display important tables"):
    
    if username and password and schema:
        USER = username
        PASSWORD = password
        WAREHOUSE = "COMPUTE_WH"
        ACCOUNT = "vqkklwj-nn52820"
        SNOWFLAKE_DATABASE = "DETENTIONDATA"

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=schema,
            role="ACCOUNTADMIN"
        )

        cur = conn.cursor()

        # List of important tables to display
        important_tables = ["Table_1", "Table_4", "Table_5", "Table_6", "Table_8"]

        for table_name in important_tables:
            try:
                # Always sort by Release_date
                if table_name in ["Table_1","Table_6"]:
                    # Table_1 has Facility Type
                    query = f'''
                        SELECT * FROM {schema}.{table_name}
                        ORDER BY "Release_date" DESC, "Facility Type" ASC
                    '''
                elif table_name == "Table_4":
                    # Other tables only sorted by Release_date
                    query = f'''
                        SELECT * FROM {schema}.{table_name}
                        ORDER BY "Release_date" DESC, "Arresting Agency" ASC
                    '''
                elif table_name == "Table_5":
                    query=f'''
                        SELECT * FROM {schema}.{table_name}
                        ORDER BY "Release_date" DESC,"Year_Month" ASC,"Agency" ASC
                    '''
                else:
                    query = f'''
                        SELECT * FROM {schema}.{table_name}
                        ORDER BY "Release_date" DESC
                    '''
                cur.execute(query)
                df = cur.fetch_pandas_all()

                st.subheader(table_name)
                st.dataframe(df)

                # Optional CSV Download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=f"Download {table_name} as CSV",
                    data=csv,
                    file_name=f"{table_name}.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.error(f"Error loading {table_name}: {e}")

        cur.close()
        conn.close()
    else:
        st.warning("Please enter all credentials above.")
if st.button("Download all tables(1 to 17) from 2023-01-09 to till now"):
    if username and password and schema:
        USER = username
        PASSWORD = password
        WAREHOUSE = "COMPUTE_WH"
        ACCOUNT = "vqkklwj-nn52820"
        SNOWFLAKE_DATABASE = "DETENTIONDATA"

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=schema,
            role="ACCOUNTADMIN"
        )
        cur = conn.cursor()
        all_tables = [f"Table_{i}" for i in range(1,18)]
        output = io.BytesIO()
        with pd.ExcelWriter(output,engine='xlsxwriter') as writer:
            for table in all_tables:
                try:
                    query = f'select * from {schema}.{table}'
                    cur.execute(query)
                    df = cur.fetch_pandas_all()
                    df.to_excel(writer,sheet_name=table,index=False)
                except Exception as e:
                    st.warnings(f"Could not load {table} : {e}")
        st.success("All tables loaded successfully!")
        st.download_button(
            label="📥 Download All Tables as Excel",
            data=output.getvalue(),
            file_name="all_detention_tables.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        cur.close()
        conn.close()
    else:
        st.warnings("Please enter all snowflake credentials.")
if st.button("Visualization of Data"):
    fig1, fig2, fig3, fig4,fig5 = Visualization(username,password,schema)
    for i, fig in enumerate([fig1, fig2, fig3, fig4,fig5], start=1):
        st.pyplot(fig)  # Show figure
        buf = fig_to_bytes(fig)  # Convert to BytesIO
        st.download_button(
            label=f"Download Figure {i}",
            data=buf,
            file_name=f"figure_{i}.png",
            mime="image/png"
        )
