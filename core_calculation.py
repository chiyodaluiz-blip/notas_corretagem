import os
import time
import tabula.io as tabula
import pandas as pd
import numpy as np
from datetime import datetime
from pandas.api.types import is_string_dtype
from PyPDF2 import PdfReader


parameters_trades = {}
parameters_taxes = {}
parameters_dates = {}

# -------- PARAMETROS ORIGINAIS --------

parameters_trades['Easynvest'] = {
    "top": 48,
    "left": 380,
    "left_default": 820,
    "width": 575,
    "heights": range(150, 151, 1),
    "columns": None,
    "desired_cols": ['Especificação do Título', 'C/V', 'Quantidade', 'Preço/Ajuste'],
    "new_col_names": ['Asset', 'BuyOrSell', 'Qty', 'Price'],
    "correct_str": 'Quantidade'
}

parameters_taxes['Easynvest'] = {
    "top": 310,
    "left": 800,
    "width": 600,
    "heights": range(180, 550, 3),
    "columns": None,
    "desired_cols": ['Taxa de Liquidação', 'Taxa de Registro', 'Emolumentos', 'Total Corretagem/Despesa', 'ISS ', 'Líquido para'],
    "new_col_names": ['TaxaLiquidacao', 'TaxaRegistro', 'Emolumentos', 'TaxaOperacional', 'Impostos', 'ValorTotal'],
    "correct_str": 'Clearing (CBLC)',
    "taxes_index_str": 'Clearing (CBLC)'
}

parameters_dates['Easynvest'] = {
    "top": 420,
    "left": 46.495,
    "width": 545.034,
    "heights": range(0, 15, 1),
    "correct_str": 'Data Pregão'
}


# -------- FUNÇÕES ORIGINAIS --------

def br2us_ccy_format(df):

    if is_string_dtype(df):

        df = [x.replace('.', '') for x in df]
        df = [x.replace(',', '.') for x in df]

        df = np.array(df, dtype=np.float32)

    return df


def trades_adjustment(dfs, desired_cols, new_col_names):

    full_df = []

    for df in dfs:

        for col in desired_cols:

            next_column = df.columns[df.columns.get_loc(col)+1]

            if df[col].isnull().all() and 'Unnamed' in next_column:

                df = df.drop(col, axis=1)
                df.rename(columns={next_column: col}, inplace=True)

        useful_df = df.loc[:,desired_cols].copy()

        useful_df.columns = new_col_names

        useful_df['Qty'] = br2us_ccy_format(useful_df['Qty'])
        useful_df['Price'] = br2us_ccy_format(useful_df['Price'])

        full_df.append(useful_df)

    return pd.concat(full_df)


def get_trades(file, brokerHouse, page, last_page_flag):

    top = parameters_trades[brokerHouse]['top']

    if last_page_flag:
        left = parameters_trades[brokerHouse]['left']
    else:
        left = parameters_trades[brokerHouse]['left_default']

    width = parameters_trades[brokerHouse]['width']
    heights = parameters_trades[brokerHouse]['heights']
    columns = parameters_trades[brokerHouse]['columns']
    correct_str = parameters_trades[brokerHouse]['correct_str']

    desired_cols = parameters_trades[brokerHouse]['desired_cols']
    new_col_names = parameters_trades[brokerHouse]['new_col_names']

    for height in heights:

        df = tabula.read_pdf(
            file,
            guess=True,
            multiple_tables=True,
            stream=True,
            area=(height, top, left, width),
            columns=columns,
            pages=page,
            pandas_options={'dtype': str}
        )

        if correct_str in str(df[0].columns.values):
            break

    full_df = trades_adjustment(df, desired_cols, new_col_names)

    full_df.columns = new_col_names
    full_df.reset_index(drop=True, inplace=True)

    return full_df


def get_dates(file, brokerHouse, page):

    top = parameters_dates[brokerHouse]['top']
    left = parameters_dates[brokerHouse]['left']
    width = parameters_dates[brokerHouse]['width']
    heights = parameters_dates[brokerHouse]['heights']
    correct_str = parameters_dates[brokerHouse]['correct_str']

    for height in heights:

        dates = tabula.read_pdf(
            file,
            guess=True,
            multiple_tables=True,
            stream=True,
            area=(height, top, left, width),
            pages=page
        )

        if correct_str in str(dates[0].columns):
            break

    full_df = pd.concat(dates)

    return full_df[correct_str].unique()[0]


def pro_rata_taxes(trades, dates, taxes):

    df = trades

    df['Date'] = dates

    df['QtdeSign'] = trades['Qty']

    negSign = df['BuyOrSell'] == 'V'

    df.loc[negSign, 'QtdeSign'] = df.loc[negSign, 'QtdeSign']*-1

    df['ValorInvestido'] = df['Qty']*df['Price']

    ratio_per_trade = (df['ValorInvestido']/sum(df['ValorInvestido']))

    df['TotalFees'] = 0

    df['ValorPago'] = df['ValorInvestido']

    return df


def validation_of_sum(df,taxes,log):

    value_error = abs(
        abs(sum(np.array(df['ValorPago']))) - abs(np.array(taxes['ValorTotal']))
    )

    if value_error > 1:

        log(f"❌ ERROR day {df['Date'].unique()[0]} diff = {value_error}")

    else:

        log(f"✅ Validation OK day {df['Date'].unique()[0]}")

    return value_error


def final_adjustments(full_output):

    full_output = pd.concat(full_output)

    full_output['BuyOrSell'].replace('C', 'B', inplace=True)
    full_output['BuyOrSell'].replace('V', 'S', inplace=True)

    full_output = full_output.reset_index()

    full_output['Asset'] = [x.split(' ')[0] for x in full_output['Asset']]

    full_output['Date'] = [
        time.strftime('%Y/%m/%d', time.strptime(d, '%d/%m/%Y'))
        for d in full_output['Date']
    ]

    grouped = full_output.groupby(
        ["Date", "BuyOrSell", "Asset", "Price"], as_index=False
    ).sum()

    output_cols = ['Date', 'BuyOrSell', 'Asset', 'Qty', 'Price', 'TotalFees']

    grouped = grouped[output_cols]

    grouped = grouped.sort_values(by='Date')

    return grouped


# -------- FUNÇÃO PRINCIPAL --------

def run_notas(directory, brokerHouse, log=print):

    full_output = []

    if directory.endswith(".pdf"):
        files_in_folder = [os.path.basename(directory)]
        directory = os.path.dirname(directory)
    else:
        files_in_folder = os.listdir(directory)

    for filename in files_in_folder:

        if filename.endswith(".pdf"):

            file = os.path.join(directory, filename)

            log(f"Processing file: {filename}")

            pdf = PdfReader(file)
            n_pages = len(pdf.pages)

            last_date = '01/01/1900'
            last_trades = pd.DataFrame()

            for page in range(1, n_pages+1):

                last_page_flag = page == n_pages

                dates = get_dates(file, brokerHouse, page)

                log(f"{filename} | Date {dates} | Page {page}/{n_pages}")

                try:

                    trades = get_trades(file, brokerHouse, page, last_page_flag)

                    if dates == last_date:

                        trades = pd.concat([last_trades, trades], ignore_index=True)

                    last_trades = trades.copy()

                except Exception as e:

                    log(f"⚠️ No trades found page {page} | {e}")

                last_date = dates

                if page == n_pages:

                    output = trades

                    full_output.append(output)

    grouped = final_adjustments(full_output)

    return grouped
