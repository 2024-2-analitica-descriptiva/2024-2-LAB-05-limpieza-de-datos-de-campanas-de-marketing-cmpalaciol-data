"""
Escriba el codigo que ejecute la accion solicitada.
"""
import os
import pandas as pd
import zipfile

def clean_campaign_data():
    """
    Limpia los datos de una campaña de marketing realizada por un banco
    y genera tres archivos CSV: client.csv, campaign.csv y economics.csv.
    """
    input_dir = "files/input/"
    output_dir = "files/output/"

    # Crear la carpeta de salida si no existe
    os.makedirs(output_dir, exist_ok=True)

    # Leer y procesar archivos comprimidos
    input_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith(".zip")
    ]

    data_frames = []

    # Cargar datos desde archivos ZIP sin descomprimirlos
    for zip_file in input_files:
        with zipfile.ZipFile(zip_file) as z:
            for file_name in z.namelist():
                if file_name.endswith(".csv"):
                    with z.open(file_name) as f:
                        df = pd.read_csv(f, low_memory=False)
                        data_frames.append(df)

    # Concatenar los datos en un solo DataFrame
    data = pd.concat(data_frames, ignore_index=True)

    # Client.csv
    client = data[
        ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    ].copy()

    client["job"] = client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client["education"] = client["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # Campaign.csv
    campaign = data[
        ["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts",
         "previous_outcome", "campaign_outcome", "day", "month"]
    ].copy()

    # Mapear los meses a números
    month_mapping = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    campaign["month"] = campaign["month"].map(month_mapping).astype(int)
    campaign["day"] = campaign["day"].astype(int)

    # Corregir transformaciones de las columnas
    campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

    # Crear la columna 'last_contact_date'
    campaign["last_contact_date"] = campaign.apply(
        lambda row: f"2022-{row['month']:02d}-{row['day']:02d}", axis=1
    )

    campaign = campaign[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
    ]
    campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # Economics.csv
    economics = data[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()
