from fpdf import FPDF
import pandas as pd
import locale


def convert_to_pdf():
    # Set the locale for currency formatting
    locale.setlocale(locale.LC_ALL, "")

    # Read the processed data
    data = pd.read_pickle("data/processed/fiscal.pkl")

    # Create a PDF in landscape orientation
    pdf = FPDF(orientation="L", unit="mm", format="Letter")
    pdf.set_font("courier", size=9)

    # Set the height and width of the cell
    cell_height = 4.4
    column_names = [
        "COUNTRY",
        f"EXPORTS (DOLLARS)",
        f"IMPORTS (DOLLARS)",
        f"TRADE BALANCE (DOLLARS)",
    ]
    num_columns = len(column_names)
    cell_width = (pdf.w - pdf.l_margin - pdf.r_margin) / num_columns

    def header():
        # Add column names as headers on each page
        pdf.set_font("courier", size=10, style="B")
        pdf.cell(pdf.w / 2.14, 0, str(col_year), align="R")
        pdf.ln(h=6)

        pdf.line(pdf.l_margin, 4, pdf.w - pdf.r_margin, 4)

        for col in column_names:
            pdf.set_font("courier", size=10, style="B")
            pdf.cell(cell_width, cell_height, col, align="C")
        pdf.ln()

        # Draw a line after the column names
        pdf.line(pdf.l_margin, pdf.y, pdf.w - pdf.r_margin, pdf.y)
        pdf.ln(h=2)

    # Iterate through fiscal years and add a new page for each
    for col_year in data["Fiscal Year"].sort_values().unique():

        pdf.add_page()
        page = pdf.page_no()
        empty_page = True
        header()

        # Add data to the table
        row_count = 0
        for _, row in data[data["Fiscal Year"] == col_year].iterrows():

            curr_page = pdf.page_no()

            if row_count % 39 == 0 and empty_page == False:
                pdf.add_page()
                header()

            empty_page = False

            new_country = row["country"].upper()
            country_length = len(new_country)

            dots = "......................................."
            new_country += dots[:-country_length]

            pdf.set_font("courier", size=8)

            # Draw cell around the country name
            pdf.cell(cell_width, cell_height, str(new_country), align="L")

            exports = locale.currency(row[f"exports"], grouping=True)
            imports = locale.currency(row[f"imports"], grouping=True)
            trade_balance = locale.currency(
                row[f"exports"] - row[f"imports"], grouping=True
            )

            pdf.cell(cell_width, cell_height, str(exports), align="R")
            pdf.cell(cell_width, cell_height, str(imports), align="R")
            pdf.cell(cell_width, cell_height, str(trade_balance), align="R")

            row_count += 1

            pdf.ln()

    # Save the PDF
    pdf.output("data/processed/tables.pdf")
