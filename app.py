import streamlit as st
import tempfile
import os
from core_calculation import run_notas

st.set_page_config(page_title="Notas de Corretagem")

st.title("Processador de Notas de Corretagem")

broker = st.selectbox(
    "Selecione a corretora",
    ["Easynvest", "Modal", "Rico", "Clear"]
)

uploaded_files = st.file_uploader(
    "Upload das notas de corretagem",
    type="pdf",
    accept_multiple_files=True
)

process_button = st.button("Processar notas")

log_container = st.empty()
logs = []

def log(msg):
    logs.append(msg)
    log_container.code("\n".join(logs))

if process_button and uploaded_files:

    with tempfile.TemporaryDirectory() as tmpdir:

        for file in uploaded_files:
            path = os.path.join(tmpdir, file.name)
            with open(path, "wb") as f:
                f.write(file.getbuffer())

        log("Starting processing...\n")

        try:

            df = run_notas(tmpdir, broker, log)

            st.success("Processamento concluído")

            st.dataframe(df)

            csv = df.to_csv(index=False).encode()

            st.download_button(
                "Download CSV",
                csv,
                "resultado_notas.csv",
                "text/csv"
            )

        except Exception as e:

            log(f"ERROR: {str(e)}")
            st.error("Erro durante processamento")
