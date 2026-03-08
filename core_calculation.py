"""
    Leitura de notas de corretagem
    Versão: 7
    Autor: LF
    Data: 21/10/2024
    Feature change: added Easynvest on multiple pages
    Known bug: It will say that there are error when you have multiple pages for same trading date. The important log is the one of the last page of that trading date. The ones before will say that they are in error because it doesnt have all the trades yet.
	To-do: 
        1) get I.R.R.F. value (to add at the IRPF program)
        2) performance improvement
"""

# Import libraries
import os
import time
import tabula.io as tabula
import warnings
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
from pandas.api.types import is_string_dtype
from PyPDF2 import PdfReader



# Parameters setting
parameters_trades = {}
parameters_taxes = {}
parameters_dates = {}


parameters_trades['Rico'] = {
    "top": 33.841,
    "left": 438.441,
    "left_default": 438.441,
    "width": 557.441,
    "heights": range(239, 240),
    "columns": [],
    "desired_cols":  ['Especificação do título', 'C/V',      'Quantidade', 'Preço / Ajuste'],
    "new_col_names": ['Asset',                   'BuyOrSell', 'Qty',       'Price'],
    "correct_str": 'Quantidade'
}

parameters_taxes['Rico'] = {
    "top": 299.638,
    "left": 652.655,
    "width": 560.695,
    "heights": range(460, 461),
    "columns": [],
    "desired_cols":  ['Taxa de liquidação', 'Taxa de Registro', 'Emolumentos', 'Taxa Operacional', 'Impostos', 'Líquido para'],
    "new_col_names": ['TaxaLiquidacao',     'TaxaRegistro',     'Emolumentos', 'TaxaOperacional',  'Impostos', 'ValorTotal'],
    "correct_str": 'Clearing',
    "taxes_index_str": 'Clearing'
}

parameters_dates['Rico'] = {
    "top": 514.303,
    "left": 71.772,
    "width": 560.416,
    "heights": range(53, 54),
    "correct_str": 'Data pregão',
}

parameters_trades['Clear'] = {
    "top": 33.841,
    "left": 438.441,
    "left_default": 438.441,
    "width": 557.441,
    "heights": range(239, 240),
    "columns": [],
    "desired_cols":  ['Especificação do título', 'C/V',      'Quantidade', 'Preço / Ajuste'],
    "new_col_names": ['Asset',                   'BuyOrSell', 'Qty',       'Price'],
    "correct_str": 'Quantidade'
}

parameters_taxes['Clear'] = {
    "top": 299.638,
    "left": 652.655,
    "width": 560.695,
    "heights": range(460, 461),
    "columns": [],
    "desired_cols":  ['Taxa de liquidação', 'Taxa de Registro', 'Emolumentos', 'Taxa Operacional', 'Impostos', 'Líquido para'],
    "new_col_names": ['TaxaLiquidacao',     'TaxaRegistro',     'Emolumentos', 'TaxaOperacional',  'Impostos', 'ValorTotal'],
    "correct_str": 'Clearing',
    "taxes_index_str": 'Clearing'
}

parameters_dates['Clear'] = {
    "top": 514.303,
    "left": 71.772,
    "width": 560.416,
    "heights": range(53, 54),
    "correct_str": 'Data pregão',
}

parameters_trades['Modal'] = {
    "top": 50.947,
    "left": 505.266,
    "left_default": 505.266,
    "width": 544.797,
    "heights": range(195, 230, 3),
    "columns": [49, 59, 97, 112, 179, 199, 334, 380, 432, 478, 529],
    "desired_cols":  ['Especificação do Título', 'C/V',       'Quantidade', 'Ajuste'],
    "new_col_names": ['Asset',                   'BuyOrSell', 'Qty',        'Price'],
    "correct_str": 'Quantidade'
}


parameters_taxes['Modal'] = {
    "top": 298.109,
    "left": 642.982,
    "width": 521.234,
    "heights": range(505, 550, 3),
    "columns": [49, 59, 97, 112, 179, 199, 334, 380, 432, 478, 529],
    "desired_cols":  ['Taxa de Liquidação', 'Taxa de Registro', 'Emolumentos', 'Total Corretagem/Despesa', 'ISS ',     'Líquido para'],
    "new_col_names": ['TaxaLiquidacao',     'TaxaRegistro',     'Emolumentos', 'TaxaOperacional',          'Impostos', 'ValorTotal'],
    "correct_str": 'CBLC',
    "taxes_index_str": 'CBLC'
}

parameters_dates['Modal'] = {
    "top": 445.809,
    "left": 46.495,
    "width": 545.034,
    "heights": range(0, 15, 1),
    "correct_str": 'Data Pregão',
}

parameters_dates['Easynvest'] = {
    "top": 420,
    "left": 46.495,
    "width": 545.034,
    "heights": range(0, 15, 1),
    "correct_str": 'Data Pregão',
}

parameters_trades['Easynvest'] = {
    "top": 48,
    "left": 380, 
    "left_default": 820,
    "width": 575,
    "heights": range(150, 151, 1),
    "columns": None,
    "desired_cols":  ['Especificação do Título', 'C/V',       'Quantidade', 'Preço/Ajuste'],
    "new_col_names": ['Asset',                   'BuyOrSell', 'Qty',        'Price'],
    "correct_str": 'Quantidade'
}

parameters_taxes['Easynvest'] = {
    "top": 310,
    "left": 800,
    "width": 600,
    "heights": range(180, 550, 3),
    "columns": [49, 59, 97, 112, 179, 199, 334, 380, 432, 478, 529],
    "desired_cols":  ['Taxa de Liquidação', 'Taxa de Registro', 'Emolumentos', 'Total Corretagem/Despesa', 'ISS ',     'Líquido para'],
    "new_col_names": ['TaxaLiquidacao',     'TaxaRegistro',     'Emolumentos', 'TaxaOperacional',          'Impostos', 'ValorTotal'],
    "correct_str": 'Clearing (CBLC)',
    "taxes_index_str": 'Clearing (CBLC)'
}


def get_trades(file, brokerHouse, page, last_page_flag):
    ''' function to load the taxes table from pdf '''

    # parameters
    top = parameters_trades[brokerHouse]['top']
    if last_page_flag:
        left = parameters_trades[brokerHouse]['left']
    else:
        left = parameters_trades[brokerHouse]['left_default']
    width = parameters_trades[brokerHouse]['width']
    heights = parameters_trades[brokerHouse]['heights']
    correct_str = parameters_trades[brokerHouse]['correct_str']
    columns = parameters_trades[brokerHouse]['columns']
    desired_cols = parameters_trades[brokerHouse]['desired_cols']
    new_col_names = parameters_trades[brokerHouse]['new_col_names']

    # Keeps reading until find the correct height in which the table starts
    for height in heights:
        df = tabula.read_pdf(file,
                             guess=True,
                             multiple_tables=True,
                             stream=True,
                             area=(height, top, left, width),
                             columns=columns,
                             pages=page,
                             pandas_options={'dtype': str})

        # Validation1
        if correct_str in str(df[0].columns.values):
            break

    # find the end of the table
    if pd.isna(df[0]["Especificação do Título"].tail(1).item()):
        for left in range(round(left)-7, 20, -7):
            
            df = tabula.read_pdf(file,
                                guess=True,
                                multiple_tables=True,
                                stream=True,
                                area=(height, top, left, width),
                                columns=columns,
                                pages=page,
                                pandas_options={'dtype': str})  
            # Treatment: when the pdf is empty (brokerhouse problem when printing file)
            if df[0].empty:
                break

            # Validation2
            if not pd.isna(df[0]["Especificação do Título"].tail(1).item()):
                break

    # Adjustment on content
    full_df = trades_adjustment(df, desired_cols, new_col_names)

    # Rename columns
    full_df.columns = new_col_names
    full_df.reset_index(drop=True, inplace=True)

    return full_df


def trades_adjustment(dfs, desired_cols, new_col_names):
    ''' function to get only the useful cols & adjust the decimal and thousand separators '''

    full_df = []

    # Check the accuracy of reading process and correct if needed
    for df in dfs:
        
        for col in desired_cols:
            next_column = df.columns[df.columns.get_loc(col)+1]

            # correct columns positions
            if df[col].isnull().all() and 'Unnamed' in next_column:
                # Remove the original column
                df = df.drop(col, 1)  # '1' for columns
                # Rename the real column
                df.rename(columns={next_column: col}, inplace=True)

        # Select only the required columns, change its name and stack the dfs after corrected
        useful_df = df.loc[:,desired_cols].copy()
        useful_df.columns = new_col_names
        useful_df['Qty'] = br2us_ccy_format(useful_df['Qty'])
        useful_df['Price'] = br2us_ccy_format(useful_df['Price'])

        full_df.append(useful_df)

    # make it one big df
    full_df = pd.concat(full_df)

    return full_df


def br2us_ccy_format(df):
    if is_string_dtype(df):
        df = [x.replace('.', '') for x in df]
        df = [x.replace(',', '.') for x in df]
        df = np.array(df, dtype=np.float32)
        
    return df



def get_taxes(file, brokerHouse, page):
    ''' function to load the taxes table from pdf '''

    # Parameters
    top = parameters_taxes[brokerHouse]['top']
    left = parameters_taxes[brokerHouse]['left']
    width = parameters_taxes[brokerHouse]['width']
    heights = parameters_taxes[brokerHouse]['heights']
    correct_str = parameters_taxes[brokerHouse]['correct_str']
    taxes_index_str = parameters_taxes[brokerHouse]['taxes_index_str']
    desired_cols = parameters_taxes[brokerHouse]['desired_cols']
    new_col_names = parameters_taxes[brokerHouse]['new_col_names']

    # Keeps reading until find the correct height in which the table starts
    for height in heights:
        taxes = tabula.read_pdf(file,
                                guess=True,
                                multiple_tables=True,
                                stream=True,
                                area=(height, top, left, width),
                                pages=page,
                                pandas_options={'dtype': str})

        # Validation
        if correct_str in str(taxes[0].columns):
            break


    # find the end of the table
    if taxes[0].shape[1] == 1:
        for left in range(round(left)-8, round(left)-400, -8):
            taxes = tabula.read_pdf(file,
                                guess=True,
                                multiple_tables=True,
                                stream=True,
                                area=(height, top, left, width),
                                pages=page,
                                pandas_options={'dtype': str})  

            # Validation2
            if taxes[0].shape[1] == 2:
                break
        
    # Make the necessary adjustments
    full_df = taxes_adjustment(taxes, desired_cols, taxes_index_str)

    # Rename columns
    full_df.columns = new_col_names

    return full_df


def taxes_adjustment(taxes, desired_cols, taxes_index_str):
    ''' function to get only the useful cols & adjust the decimal and thousand separators '''

    full_df = []

    for df in taxes:

        # Skip empty tables
        if df.iloc[:, 1].isnull().all():
            continue

        # Transpose the data and set the correct index
        useful_df = df.set_index(taxes_index_str).T

        col_names = useful_df.columns
        real_col_names = [col_names[col_names.str.find(col) == 0][0] for col in desired_cols]

        # Filter columns and lines
        useful_df = useful_df[real_col_names]
        useful_df = useful_df[~useful_df.iloc[:, 0].isin(['D', 'C'])]

        # make it numeric
        for col in real_col_names:
            if is_string_dtype(useful_df[col]):
                useful_df[col] = [x.replace('.', '') for x in useful_df[col]]
                useful_df[col] = [x.replace(',', '.') for x in useful_df[col]]
                useful_df[col] = useful_df[col].astype(float)

        # Stack the dfs after corrected
        full_df.append(useful_df)

    # make it one big df (taxes are always positive)
    full_df = abs(pd.concat(full_df))

    return full_df


def get_dates(file, brokerHouse, page):
    ''' function to load the dates from pdf '''

    # parameters
    top = parameters_dates[brokerHouse]['top']
    left = parameters_dates[brokerHouse]['left']
    width = parameters_dates[brokerHouse]['width']
    heights = parameters_dates[brokerHouse]['heights']
    correct_str = parameters_dates[brokerHouse]['correct_str']

    # Keeps reading until find the correct height in which the table starts
    for height in heights:
        dates = tabula.read_pdf(file,
                                guess=True,
                                multiple_tables=True,
                                stream=True,
                                area=(height, top, left, width),
                                pages=page)

        # Validation
        if correct_str in str(dates[0].columns):
            break

    # make it one big df
    full_df = pd.concat(dates)
    full_df = full_df[correct_str].unique()[0]

    return full_df


def run_notas(directory, brokerHouse, progress_callback=None):
    ''' main function: load tables, process them and save it '''

    full_output = []

    if directory.endswith(".pdf"):
        files_in_folder = [os.path.basename(directory)]
        directory = os.path.dirname(directory)
    else:
        files_in_folder = os.listdir(directory)

    # -------- count total pages (for progress) --------

    total_pages = 0

    for filename in files_in_folder:
        if filename.endswith(".pdf"):
            file = os.path.join(directory, filename)
            pdf = PdfReader(file)
            total_pages += len(pdf.pages)

    processed_pages = 0

    # --------------------------------------------------

    for filenaame in tqdm(files_in_folder):

        if filename.endswith(".pdf"):

            file = os.path.join(directory, filename)

            pdf = PdfReader(file)
            n_pages = len(pdf.pages)

            last_date = '01/01/1900'
            last_trades = pd.DataFrame()

            for page in tqdm(range(1, n_pages+1), leave=False):

                last_page_flag = False

                if page == n_pages:
                    last_page_flag = True

                dates = get_dates(file, brokerHouse, page)

                print("\n" + "📄Processing file "+ filename)
                print("\n\t" + " - Trade date: " + dates)
                print("\n\t" + " - Page: " + str(page) + "/" + str(n_pages))

                try:

                    trades = get_trades(file, brokerHouse, page, last_page_flag)

                    if dates == last_date:
                        trades = pd.concat([last_trades, trades], ignore_index=True).reset_index(drop=True).copy()

                    last_trades = trades.copy()
                    print("\n\t #Trades found for file " + filename + " on page " + str(page) + ": " + str(len(last_trades)))

                except:

                    print("\n\t⚠️No trades found for file " + filename + " on page " + str(page))

                last_date = dates

                if page == n_pages:

                    taxes = get_taxes(file, brokerHouse, page)

                    output = pro_rata_taxes(trades, dates, taxes)

                    value_error = validation_of_sum(output, taxes)

                    if value_error < 1:
                        full_output.append(output)

                # ---------- progress update ----------

                processed_pages += 1

                if progress_callback and total_pages > 0:
                    progress_callback(processed_pages / total_pages)

                # -------------------------------------

    grouped = final_adjustments(full_output, directory)

    grouped.to_excel(
        os.path.join(
            directory,
            'outputGroupedByTicker_' +
            datetime.now().strftime("%Y_%m_%d-%I_%M_%p") + '.xlsx'
        )
    )

    return grouped


def pro_rata_taxes(trades, dates, taxes):
    ''' function to calculate the taxes by asset, according to the invested value (pro-rata) '''

    df = trades
    df['Date'] = dates

    # Set the qty with sign
    df['QtdeSign'] = trades['Qty']
    negSign = df['BuyOrSell'] == 'V'
    df.loc[negSign, 'QtdeSign'] = df.loc[negSign, 'QtdeSign']*-1

    # calculate the total amount column
    df['ValorInvestido'] = df['Qty']*df['Price']

    # Proportional taxes
    ratio_per_trade = (df['ValorInvestido']/sum(df['ValorInvestido']))
    df['TxLiq'] = np.array(taxes['TaxaLiquidacao'])*ratio_per_trade
    df['TxReg'] = np.array(taxes['TaxaRegistro'])*ratio_per_trade
    df['Emolumentos'] = np.array(taxes['Emolumentos'])*ratio_per_trade
    df['Impostos'] = np.array(taxes['Impostos'])*ratio_per_trade
    df['TxOp'] = np.array(taxes['TaxaOperacional'])*ratio_per_trade
    
    # Sum all fees
    df['TotalFees'] = df['TxLiq'] + df['TxReg'] + \
        df['Emolumentos'] + df['Impostos'] + df['TxOp']

    # Final calculation
    df['ValorPago'] = 0
    # Sell side
    df.loc[negSign, 'ValorPago'] = df['ValorInvestido'] - df['TotalFees']
    # Buy side
    df.loc[~negSign, 'ValorPago'] = df['ValorInvestido'] + df['TotalFees']

    
    df['ValorPagoSign'] = df['ValorPago']
    df.loc[negSign, 'ValorPagoSign'] = df.loc[negSign, 'ValorPagoSign']*-1

    return df

def validation_of_sum(df,taxes):
    
    # Validation #1: is the total value calculated equal to the one in the note?
    value_error = abs(
        abs(sum(np.array(df['ValorPagoSign']))) - abs(np.array(taxes['ValorTotal'])))
    if value_error > 1:
        print('\n\t❌ERROR in day ' + df['Date'].unique()[0]
              + '! The diff between calculated and real values is R$ ' + str(value_error))
    else:
        print('\n\t✅Validation OK for day ' + df['Date'].unique()[0])
    return value_error


def final_adjustments(full_output, directory):
    ''' function adjust the Buy/Sell symbols, date formats & groupby the results '''

    # make it one big df and save as excel
    full_output = pd.concat(full_output)
    full_output['BuyOrSell'].replace('C', 'B', inplace=True)
    full_output['BuyOrSell'].replace('V', 'S', inplace=True)

    full_output = full_output.reset_index()

    # Adjust the names
    full_output['Asset'] = [full_output['Asset'][i].split(
        ' ')[0] for i in range(0, len(full_output))]

    # Adjust the dates
    full_output['Date'] = [time.strftime('%Y/%m/%d', time.strptime(
        full_output['Date'][i], '%d/%m/%Y')) for i in range(0, len(full_output))]

    # Adjust to summarize trades
    grouped = full_output.groupby(
        ["Date", "BuyOrSell", "Asset", "Price"], as_index=False).sum()

    # Set columns order
    output_cols = ['Date', 'BuyOrSell', 'Asset', 'Qty', 'Price', 'TotalFees']
    full_output = full_output[output_cols]
    grouped = grouped[output_cols]

    # Order by date
    grouped = grouped.sort_values(by='Date')

    return grouped
