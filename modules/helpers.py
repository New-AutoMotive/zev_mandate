"""
File containing common python methods used in this project.
"""
# Packages
import pandas as pd

# Modules


def ft_cleaner_dft(string: str) -> str:
    """
    Iterate though dict and replace key instances with new values.
    Args:
        string (str): String to review and replace incorrect words.
    Returns:
        str: String with all unwanted instances replaced.
    """
    replacements = {
        "Battery electric": "Pure Electric",
        "Other fuels": "Other",
        "Other fuel types": "Other",
        "Plug-in hybrid electric (diesel)": "Diesel",
        "Range extended electric": "Pure Electric",
        "Plug-in hybrid electric (petrol)": "Petrol",
        "Hybrid electric (petrol)": "Petrol",
        "Hybrid electric (diesel)": "Diesel",
        "Fuel cell electric": "Pure Electric",
        "Gas": "Other"
    }
    for key, value in replacements.items():
        string = string.replace(key, value)
    return string


def clean_traffic_data(request) -> pd.DataFrame:
    """
    Convert the Traffic Data request into a DataFrame and clean.
    Args:
        request: The result of the request.get() call to the Traffic Data API.
    Returns:
        pd.DataFrame: A DataFrame of the data collected from the request.
    """
    df = pd.DataFrame(request.json()["data"])
    df["year"] = [pd.to_datetime(year, format="%Y") for year in df["year"]]
    df.set_index("year", inplace=True)
    df = df.resample("Y").last()
    return df


def clean_new_reg_data(df_nrg: pd.DataFrame) -> pd.DataFrame:
    """
    Convert new registrations DataFrame to car count dataframe.
    Args:
        df_nrg (pd.DataFrame): DataFrame created from the new registrations CSV.
    Returns:
        pd.DataFrame: DataFrame in car count style.
    """
    df = df_nrg[
        (df_nrg["Geography"] == "Wales") &
        (df_nrg["Date Interval"] == "Monthly") &
        (df_nrg["Units"] == "Thousands") &
        (df_nrg["BodyType"] == "Cars")
    ]
    
    df["Date"] = [pd.to_datetime(date) for date in df["Date"]]
    
    df = df.drop(columns=["Total", "Plug-in", "Zero Emission"])
    df = df.melt(id_vars=["Date", "Geography", "Date Interval", "Units", "BodyType"], value_vars=df.columns[5:]).rename(columns={
        "variable": "fuelType"
    })
    
    df["fuelType"] = df["fuelType"].apply(ft_cleaner_dft)
    df = df.groupby(["Date", "fuelType"]).sum().reset_index(["fuelType"]).pivot(columns="fuelType").resample("Y").sum()
    df.columns = [column[1] for column in df.columns]
    df = 1000 * df
    return df


def get_transport_from_emissions_df(df_em: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the Welsh emissions DataFrame to contain only the data we need.
    Args:
        df_em (pd.DataFrame): DataFrame made from Welsh emissions data.
    Returns:
        pd.DataFrame: DataFrame made up of transportation related emissions.
    """
    df_em["NCFormat"] = df_em["NCFormat"].fillna(method="ffill")
    df_em = df_em.melt(id_vars=["NCFormat", "IPCC_name"])
    df_em = df_em[df_em["variable"] != "BaseYear"]
    df_em["variable"] = [pd.to_datetime(var) for var in df_em["variable"]]
    df_emt = df_em[df_em["NCFormat"] == "Transport"]
    df_emt = df_emt.pivot(index="variable", columns="IPCC_name", values="value")
    return df_emt


def prep_mm(df: pd.DataFrame) -> pd.DataFrame:
    df['year'] = df.year.apply(lambda x: pd.to_datetime(x, format='%Y'))
    df = df[(df['annual_mileage'] > 0) & (df['year'] > '2010-01-01')]
    df = df.pivot(index='year', columns='fuelType', values='annual_mileage')
    df = df.resample('Y').last()
    df = df[['Diesel', 'Petrol', 'Electric']].rename(columns={'Diesel': 'Diesel_miles', 'Petrol': 'Petrol_miles', 'Electric': 'Electric_miles'})
    return df


def prep_df_pc(df_pc: pd.DataFrame, df_mm: pd.DataFrame) -> pd.DataFrame:
    """
    Run data preparation steps on df_pc.
    Args:
        df_fc (pd.DataFrame): The dataframe df_pc referring to vehicle parc data.
        df_mm (pd.DataFrame): The dataframe df_mm referring to annual milage.
    Returns:
        pd.DataFrame: df_pc in the prepared form.
    """
    df_mm = prep_mm(df_mm)
    df_pc.index = pd.to_datetime(df_pc.index)
    cars = df_pc[df_pc.BodyType == 'Cars'].pivot(columns='Fuel', values='value').resample('Y').last()
    
    df = cars.merge(df_mm, how='left', left_index=True, right_index=True)
    
    df['diesel_vmt'] = df.apply(lambda row: row['Diesel'] * row['Diesel_miles'], axis=1)
    df['petrol_vmt'] = df.apply(lambda row: row['Petrol'] * row['Petrol_miles'], axis=1)
    df['electric_vmt'] = df.apply(lambda row: row['Pure Electric'] * row['Electric_miles'], axis=1)
    
    return df



def prep_df_fc(df_fc: pd.DataFrame, df_pc: pd.DataFrame, df_mm: pd.DataFrame) -> pd.DataFrame:
    """
    Run data preparation steps on df_fc.
    Args:
        df_fc (pd.DataFrame): The dataframe df_fc referring to fuel consumption.
        df_pc (pd.DataFrame): The dataframe df_pc referring to vehicle parc.
        df_mm (pd.DataFrame): The dataframe df_mm referring to annual milage.
    Returns:
        pd.DataFrame: df_fc in the prepared form.
    """
    df_fc.index = pd.to_datetime(df_fc.index)
    
    df_pc = prep_df_pc(df_pc, df_mm)
    
    df_fc = df_pc.merge(
        df_fc[["Diesel cars total", "Petrol cars total"]].rename(
            columns={
                "Diesel cars total": "car_diesel_consumption",
                "Petrol cars total": "car_petrol_consumption"
            }
        ).resample("Y").last(),
        how="left",
        left_index=True,
        right_index=True
    )
    
    df_fc['car_diesel_consumption_gallons'] = df_fc.car_diesel_consumption.apply(lambda x: ((1000*x)/.98) * 219.969)
    # NB that petrol has a different conversion factor. Tonnes of oil equivalent is essentially the energy content of the fuel, and diesel is more energy-dense. 
    df_fc['car_petrol_consumption_gallons'] = df_fc.car_petrol_consumption.apply(lambda x: ((1000*x)/.86) * 219.969)
        
    df_fc['petrol_economy'] = df_fc.apply(lambda row: row['petrol_vmt'] / row['car_petrol_consumption_gallons'] , axis=1)
    df_fc['diesel_economy'] = df_fc.apply(lambda row: row['diesel_vmt'] / row['car_diesel_consumption_gallons'], axis=1)
    
    # From 2022 factors.
    df_fc['diesel_emissions'] = df_fc.car_diesel_consumption.apply(lambda x: ((1000*x)*11629.9998357937)*.24115)
    df_fc['petrol_emissions'] = df_fc.car_petrol_consumption.apply(lambda x: ((1000*x)*11629.9998357937)*.22719)
    
    return df_fc
