import streamlit as st
import tempfile
import os
import pandas as pd
import io
import sys
from contextlib import redirect_stdout

from core_calculation import run_notas

st.title("Processador de Notas de Corretagem (IRPF)")

broker = st.selectbox(
    "Selecione a corretora",
    ["Easynvest", "Modal", "Rico", "Clear"]
)

uploaded_files = st.file_uploader(
    "Upload das notas de corretagem (PDF)",
    type="pdf",
    accept_multiple_files=True
)

if uploaded_files:

    if st.button("Processar notas"):

        log_container = st.empty()
        log_buffer = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:

            for file in uploaded_files:
                path = os.path.join(tmpdir, file.name)
                with open(path, "wb") as f:
                    f.write(file.getbuffer())

            try:

                with redirect_stdout(log_buffer):

                    df = run_notas(tmpdir, broker)

            except Exception as e:

                st.error("Erro durante processamento")
                st.exception(e)
                st.stop()

        logs = log_buffer.getvalue()

        st.subheader("Logs de processamento")
        st.text(logs)

        st.success("Processamento concluído")

        st.subheader("Resultado final")

        st.dataframe(df)

        csv = df.to_csv(index=False).encode()

        st.download_button(
            "Download CSV",
            csv,
            "notas_processadas.csv",
            "text/csv"
        )
