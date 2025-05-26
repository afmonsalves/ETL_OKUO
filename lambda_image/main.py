from config import get_settings
from s3_client import S3Client
import pandas as pd

# Load settings & client
settings = get_settings()  
client = S3Client(settings)

# Download the CSV into a DataFrame
df = client.download_csv(client._input_path)
print("Data download successful. DataFrame shape:", df.shape)

#fecha_compra to datetime
df['fecha_compra'] = pd.to_datetime(df['fecha_compra'], format='%Y-%m-%d')

#create a temporary DataFrame to group by fecha_compra and usuario and find recurrency
df_temp = df.copy()

#define threshold of products per purchase for recurrent purchases (we will call this "important" purchases)
min_products = 10

#group by fecha and usuario and the sum producto
df_temp = df_temp.groupby(['fecha_compra', 'usuario'])['cantidad'].sum().reset_index()
df_temp = df_temp.sort_values(by='fecha_compra')
df_temp = df_temp[df_temp['cantidad'] > min_products]

#To find recurrent purchases, we will check if a user has made important purchases in a sliding window of 30 days
num_dates = 30    # number of days in the sliding window
min_compras = 2   # minimun number of times a user must have made an important purchase in the sliding window to be considered recurrent
date_range = pd.date_range(start=df_temp['fecha_compra'].min()
                        , end=df_temp['fecha_compra'].max() - pd.DateOffset(days=num_dates))
recurrentes = []
for date in date_range:
    temp = df_temp[(df_temp['fecha_compra'] >= date) 
                    & (df_temp['fecha_compra'] <= date + pd.DateOffset(days=num_dates))]
    #store the usuarios whose values_count is greater than min_compras
    usuarios = temp['usuario'].value_counts()
    usuarios = usuarios[usuarios > min_compras].index.tolist()
    recurrentes.extend(usuarios)

# Remove duplicates
recurrentes = set(recurrentes)

# Filter the original DataFrame to keep only recurrent users
df['recurrente'] = df['usuario'].isin(recurrentes).astype(int)
# Find day of the week (this will be used for deeper analysis)
df['dia_semana'] = df['fecha_compra'].dt.day_name()

# Start analysis for recurrent users
df_rec = df[df['recurrente'] == 1].copy()
#for each usuario, find the most frequent dia_semana
df_rec['dia_mas_frecuente'] = df_rec.groupby('usuario')['dia_semana'].transform(lambda x: x.mode()[0] if not x.mode().empty else None)
#group by usuario and producto and sum cantidad and include dia mas frecuente
df_rec = df_rec.groupby(['usuario', 'producto','dia_mas_frecuente'])['cantidad'].sum().reset_index()
#for each usuario, sort producto by cantidad
df_rec = df_rec.sort_values(by=['usuario', 'cantidad'], ascending=[True, False])
#find probability of each producto for each usuario
df_rec['probabilidad_compra'] = df_rec.groupby('usuario')['cantidad'].transform(lambda x: (x / x.sum())*100)
#keep only the top 3 productos for each usuario
df_rec = df_rec.groupby('usuario').head(3)

# Upload the cleaned DataFrame to S3 in Parquet format
client.upload_parquet(df_rec, client._output_path)

# Print success message
print("Clean data upload successful. DataFrame shape:", df_rec.shape)