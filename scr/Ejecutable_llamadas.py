import os
import pandas as pd
from pathlib import Path
  

def get_data(filename):
    df=pd.read_csv(f"gs://jca_llamadas_123/Data/raw/{filename}",sep=";",encoding="latin-1")
    return df
def change_nulls(df):

    for column in df.columns:
            df[column].fillna('SIN_DATOS',inplace=True)
          
        #else:
         #    print("otro")
    return df 

def change_type(df):

    for columna in df.columns:
        if df[columna]=='FECHA_INICIO_DESPLAZAMIENTO_MOVIL':
            df[col] = pd.to_datetime(df[col],errors='coerce')
    return df



def Borrar_d(df):

    df=df.drop_duplicates()
    return df  


     
def generate_file(df,file_name):

    out_name='Reporte_' + file_name
    #out_path=os.path.join(bucket,"data","processed",out_name)
    df.to_csv(f'gs://jca_llamadas_123/Data/processed/{out_name}')
    #reporte.to_csv(out_path)
    table_name='esp_big_jca.reporte_llamadas'
    df.to_gbq(table_name,if_exists='replace')

def main ():
    filenames = ["llamadas123_agosto_2022.csv","llamadas123_julio_2022.csv"]
    dfr = []
    for filename in filenames:
        df1 = get_data(filename)
        df1 = change_nulls(df1)
        #df1 = change_type(df1,'FECHA_INICIO_DESPLAZAMIENTO_MOVIL')
        df1 = Borrar_d(df1)
     
        
        dfr.append(df1)
 
    Datos = pd.concat(dfr)
    generate_file(Datos, "Consolidado.csv")

        
if __name__=='__main__':
    main()