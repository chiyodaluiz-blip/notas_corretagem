import streamlit as st
import tempfile
import os
import pandas as pd

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

        with tempfile.TemporaryDirectory() as tmpdir:

            for file in uploaded_files:
                path = os.path.join(tmpdir, file.name)
                with open(path, "wb") as f:
                    f.write(file.getbuffer())

            df = run_notas(tmpdir, broker)

        st.success("Processamento concluído")

        st.dataframe(df)

        csv = df.to_csv(index=False).encode()

        st.download_button(
            "Download CSV",
            csv,
            "notas_processadas.csv",
            "text/csv"
        )
