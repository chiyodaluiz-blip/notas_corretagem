import streamlit as st
import tempfile
import os
import pandas as pd
import sys

from core_calculation import run_notas


class StreamlitLogger:
    def __init__(self, container):
        self.container = container
        self.logs = ""

    def write(self, message):
        if message.strip():
            self.logs += message
            self.container.text(self.logs)

    def flush(self):
        pass


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

        log_box = st.empty()
        progress_bar = st.progress(0)

        def update_progress(p):
            progress_bar.progress(min(int(p * 100), 100))

        with tempfile.TemporaryDirectory() as tmpdir:

            for file in uploaded_files:
                path = os.path.join(tmpdir, file.name)
                with open(path, "wb") as f:
                    f.write(file.getbuffer())

            logger = StreamlitLogger(log_box)

            old_stdout = sys.stdout
            sys.stdout = logger

            try:

                df = run_notas(
                    tmpdir,
                    broker,
                    progress_callback=update_progress
                )

            except Exception as e:

                sys.stdout = old_stdout
                st.error("Erro durante processamento")
                st.exception(e)
                st.stop()

            sys.stdout = old_stdout

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
