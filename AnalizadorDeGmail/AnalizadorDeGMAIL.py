import os
import sys
import imaplib # Biblioteca para interactuar con servidores de correo electrónico usando el protocolo IMAP
import email # Biblioteca para manejar correos electrónicos
from email.header import decode_header # Función para decodificar encabezados de correo electrónico

import pandas as pd # Biblioteca para manipulación y análisis de datos
from tqdm import tqdm # Biblioteca para añadir barras de progreso

import numpy as np # Biblioteca para operaciones numéricas y matrices
from PIL import Image # Biblioteca para abrir, manipular y guardar imágenes

import matplotlib.pyplot as plt # Biblioteca para crear gráficos y visualizaciones
from matplotlib.colors import LinearSegmentedColormap # Función para crear mapas de colores segmentados
import seaborn as sns


################# RESUMEN #################

#- Scrapeo/Obtencion de datos
#- Limpieza de datos
#- Gestion ficheros
#- Visualizacion

###########################################



############## CONFIGURACION ##############

bQuieroScrapear = False # Si quiero que empieza a scrapear/obtener-datos = True, y si no quiero porque lo tengo en un csv = False
# Configuramos la ruta donde queremos que se guarde los datos sin limpiar y el nombre del .csv 
sRutaYDatosEnBruto = r'C:\Users\pepe\xxxxx\xxxxx\Data\dfEmail.csv' # Si bQuieroScrapear es False, carga los datos de este .csv en el dataframe

sUser = "pepe@gmail.com" # sUser = Cuenta que queremos analizar
sPassword = "xxxx xxxx xxxx xxxx"# # sPassword = Contraseña de aplicacion (Se llama asi) generada para este proyecto

sCarpetaDestinoFinal = r'C:\Users\pepe\xxxxx\xxxxx\Data'
sNombreFichero = 'dfEmail_Final.csv' # Nombre del .cvs que se guardara para almacenar los datos limpios


###########################################



########  SCRAPEO/OBTENCION-DATOS #########

def fScrapearCorreos(sUser, sPassword, sRutaYDatosEnBruto): 
    ####### Paso 1: Acceder a Gmail con las credenciales
    sImapUrl ='imap.gmail.com'
    oMyMail = imaplib.IMAP4_SSL(sImapUrl)
    
    bContinue = True
    dfEmail = ""
    try:
        oMyMail.login(sUser, sPassword)
    except Exception as e:
        print(f'\n - Error al loguearse: {e} \n')
        bContinue = False

    if bContinue:
        # Ver cuantos correos tengo en la Bandeja de Entrada
        try:
            iTotalCorreos = int(oMyMail.select('Inbox')[1][0].decode('utf-8'))
            print(f'- iTotalCorreos: {iTotalCorreos} \n')
        except Exception as e:
            print(f'\n - Error al seleccionar la bandeja de entrada: {e} \n')
            bContinue = False

    if bContinue:
        ####### Paso 2: Obtener correos electrónicos
        # Crear un DataFrame vacío
        dfEmail = pd.DataFrame(columns=['Date', 'From', 'Subject'])

        # Lista para almacenar los datos
        lRows = []

        for i in tqdm(range(iTotalCorreos), desc="Processing", unit="item", ncols=60, bar_format='{l_bar}{bar} | Time: {elapsed} | {n_fmt}/{total_fmt}'):
            try:
                sData = oMyMail.fetch(str(i), '(UID RFC822)')
                tArray = sData[1][0]

                if isinstance(tArray, tuple):
                    try:
                        sMsg = email.message_from_string(str(tArray[1], 'utf-8'))
                    except UnicodeDecodeError:
                        sMsg = email.message_from_string(str(tArray[1], 'latin-1'))

                    lRows.append({"Date": sMsg['Date'], "From": sMsg['from'], "Subject": sMsg['subject']})
            except Exception as e:
                print(f'\n - Error al obtener el correo {i}: {e} \n')

        # Convertir la lista de filas a un DataFrame
        dfEmail = pd.DataFrame(lRows, columns=['Date', 'From', 'Subject'])

        # Limpiar filas vacías
        dfEmail = dfEmail.dropna(how='all').reset_index(drop=True)

        # Mostrar el DataFrame
        #print(dfEmail)

        dfEmail.to_csv(sRutaYDatosEnBruto, index=False, encoding='utf-8')

        # Para ver las variables que podemos extraer
        print(f'- sMsg.keys(): \n {sMsg.keys()} \n')

    return bContinue, dfEmail, iTotalCorreos



if bQuieroScrapear:
    bContinue, dfEmail, iTotalCorreos = fScrapearCorreos(sUser, sPassword, sRutaYDatosEnBruto)
    if not bContinue:
        sys.exit()
else:
    print("\n - Scrapeador Desactivado - \n")
    try:
        # Cargar los datos desde un archivo CSV en la variable dfEmail
        dfEmail = pd.read_csv(sRutaYDatosEnBruto)
        iTotalCorreos = len(dfEmail)
    except Exception as e:
        print(f'\n - Error al cargar el archivo CSV: {e} \n')

###########################################




############ LIMPIEZA DE DATOS ############

def fLimpiarFecha(x):
    if ',' not in x: x = ', ' + x
    if '(' in x: x = ' '.join(x.split(' ')[:-1])
    x = ' '.join(x.split(' ')[:-1])
    return x

# Transformar Date "Wed, 14 Sep 2022 17:38:23 +0000 (UTC)" 
# Obtener columna 'H_M_S'
dfEmail['Date'] = dfEmail['Date'].apply(fLimpiarFecha)      # Se obtiene "Wed, 14 Sep 2022 17:38:23"
dfEmail['Date'] = dfEmail['Date'].str.split(', ').str[-1]   # Se obtiene "14 Sep 2022 17:38:23"
dfEmail['H_M_S'] = dfEmail['Date'].apply(lambda x: x[-8:])  # Se obtiene "17:38:23"

# Obtener columna 'Hour'
dfEmail['Hour'] = dfEmail['H_M_S'].apply(lambda x: x[:2]+'h-'+str(int(x[:2])+1).zfill(2)+'h')    # Se obtiene "17h-18h"

# Obtener columna 'Date'
dfEmail['Date'] = dfEmail['Date'].apply(lambda x: x[:-9] if len(x[:-9])==11 else '0'+x[:-9] )    # Se obtiene "14 Sep 2022"
dfEmail['Date'] = pd.to_datetime(dfEmail['Date'], format='%d %b %Y')                             # Se obtiene "2022-09-14"

# Obtener columna 'WeekDay'
dfEmail['WeekDay'] = dfEmail['Date'].dt.strftime('%A')                                           # Se obtiene "Wednesday"  
#print(f'dfEmail.head(): \n {dfEmail.head()} \n\n')






def fObtenerCorreoDeFrom(sMail):
    """Extrae la dirección de correo electrónico de una cadena de texto con formato 'Nombre <correo@example.com>'."""
    try:
        return sMail.split('<')[-1].split('>')[0]
    except Exception as e:
        print(f'\n - Error al extraer el correo: {e} \n')
        return ""


def fObtenerNombreDeFrom(sName):
    """Extrae el nombre de una cadena de texto con formato 'Nombre <correo@example.com>'."""
    try:
        sTexto, encoding = decode_header(sName)[0]
        if not encoding and isinstance(sTexto, str):
            sTexto = ' '.join(sTexto.split(' ')[:-1])
        else:
            sTexto = sTexto.decode('utf-8', errors='ignore')
        return sTexto.replace('"', '')
    except Exception as e:
        print(f'\n - Error al extraer el nombre: {e} \n')
        return ""


def fLimpiarSubject(sSubject):
    """Limpia el campo de 'Subject' decodificando y eliminando caracteres no deseados."""
    if isinstance(sSubject, float):
        # Maneja valores NaN o cualquier float
        return ""  
    if sSubject:
        try:
            sTexto, encoding = decode_header(sSubject)[0]
            sTexto = sTexto.decode('utf-8', errors='ignore') if encoding else sTexto
        except Exception as e:
            print(f'\n - Error al limpiar el subject: {e} \n')
            sTexto = sSubject
    else:
        sTexto = sSubject
    return sTexto


# Contar subjects que son NaN
iSubjectNaN = dfEmail['Subject'].isna().sum()
print(f'- Número de subjects que son NaN: {iSubjectNaN} \n')


dfEmail['Mail'] = dfEmail['From'].apply(fObtenerCorreoDeFrom)
dfEmail['Name'] = dfEmail['From'].apply(fObtenerNombreDeFrom)
dfEmail['Subject'] = dfEmail['Subject'].apply(fLimpiarSubject)
dfEmail = dfEmail.drop(columns=['From'])[['Date','H_M_S','Hour','WeekDay','Mail','Name','Subject']]
#print(f'dfEmail.head(): \n {dfEmail.head()} \n\n')

###########################################



########### GESTION DE FICHEROS ###########

# Asegurarse de que el directorio 'Data' exista
if os.path.exists(sCarpetaDestinoFinal) == False:
    os.makedirs(sCarpetaDestinoFinal)

# Concatenar la ruta de la carpeta y el fichero
sCarpetaYFichero = os.path.join(sCarpetaDestinoFinal, sNombreFichero)

if os.path.exists(sCarpetaYFichero):
    iContador = 1
    # Quitamos la extension al nombre
    sNombreSinExtension = sNombreFichero.rsplit('.', 1)[0]
    # Este bucle busca si existe un fichero con el mismo nombre
    while os.path.exists(os.path.join(sCarpetaDestinoFinal, f'{sNombreSinExtension}_{iContador}.csv')):
        iContador += 1
    sCarpetaYFichero = os.path.join(sCarpetaDestinoFinal, f'{sNombreSinExtension}_{iContador}.csv')
    #Creamos el csv con los datos limpios
    dfEmail.to_csv(sCarpetaYFichero, index=False, encoding='utf-8')
    print(f'- Archivo guardado como: {sCarpetaYFichero}')
else:
    #Creamos el csv con los datos limpios
    dfEmail.to_csv(sCarpetaYFichero, index=False, encoding='utf-8')
    print(f'- Archivo guardado como: {sCarpetaYFichero}')
    
###########################################



######### VISUALIZACION DE DATOS ##########
############# DATOS BASICOS #############

def fDatosBasicos():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    print(f"1. dfEmail.iloc: \n {dfEmail.iloc[1:4]} \n") #Ver las primeras 4 lineas del dataframe

    print("---------------------------- \n")

    print(f"2. dfEmail.columns: \n {dfEmail.columns} \n") #Ver las columnas del dataframe

    print("---------------------------- \n")

    print(f"3. dfEmail.describe: \n {dfEmail.describe(include='all')} \n") #Ver datos estadisticos del dataframe

    print("---------------------------- \n")


############ Graficos de Barras ###########

# - Cantidad de Correos por año
# - Cantidad de Correos por mes
# - Cantidad de Correos por dia
# - Cantidad de Correos por hora
# - Top 10 de quien manda mas correos

###########################################

def fHistplotAnual(dfEmail, iTotalCorreos):
    # Cantidad de Correos por año
    lCorreosPorAño = dfEmail.groupby(dfEmail['Date'].dt.year)['Date'].count()
    lCorreosPorAño.plot(kind='bar', xlabel='Año', ylabel='Cantidad de Correos', title=f'Cantidad de Correos por Año - Total: {iTotalCorreos}')
    plt.show()



def fHistplotMensual(dfEmail, iTotalCorreos):
    
    # Crear un diccionario para mapear los números de mes a sus nombres
    dNombreMeses = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}

    # Crear una nueva columna con el nombre del mes
    dfEmail['NombreMes'] = dfEmail['Date'].dt.month.map(dNombreMeses)

    # Crear una nueva columna con el año
    dfEmail['Año'] = dfEmail['Date'].dt.year

    # Agrupar por mes y año, y contar la cantidad de correos
    lCorreosPorMesYAño = dfEmail.groupby(['NombreMes', 'Año']).size().unstack(fill_value=0)

    # Crear un orden para los meses
    orden_meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                   'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    
    # Reindexar el DataFrame para que los meses estén en el orden correcto
    lCorreosPorMesYAño = lCorreosPorMesYAño.reindex(orden_meses)

    # Graficar
    lCorreosPorMesYAño.plot(kind='bar', stacked=True, cmap='viridis')
    plt.xlabel('Mes')
    plt.ylabel('Cantidad de Correos')
    plt.title(f'Cantidad de Correos por Mes (Dividido por Año) - Total: {iTotalCorreos}')
    plt.title('Cantidad de Correos por Mes (Dividido por Año)')
    plt.xticks(rotation=45)
    plt.legend(title='Año', bbox_to_anchor=(1, 1), loc='upper left') 
    plt.show()



def fHistplotSemanal(dfEmail, iTotalCorreos):
    # Diccionario de traducción de días de la semana
    lTraduccionDias = {
        'Monday': 'Lunes',
        'Tuesday': 'Martes',
        'Wednesday': 'Miércoles',
        'Thursday': 'Jueves',
        'Friday': 'Viernes',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }

    # Orden de los días de la semana
    lOrdenDiasSemana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

    # Aplicar la traducción a la columna WeekDay
    dfEmail['WeekDay'] = dfEmail['WeekDay'].map(lTraduccionDias)
    # Cantidad de Correos por día de la semana
    lCorreosPorDiaSemana = dfEmail.groupby(dfEmail['WeekDay'])['Date'].count()
    # Reindexar la Serie con el orden deseado
    lCorreosPorDiaSemana = lCorreosPorDiaSemana.reindex(lOrdenDiasSemana)
    lCorreosPorDiaSemana.plot(kind='bar', xlabel='Día de la Semana', ylabel='Cantidad de Correos', title=f'Cantidad de Correos por Día de la Semana - Total: {iTotalCorreos}')
    plt.show()



def fHistplotHoras(dfEmail, iTotalCorreos):
    # Extraer la hora de la columna 'H_M_S' y agregar "h" al final
    dfEmail['Hora'] = dfEmail['H_M_S'].str.split(':').str[0].map(lambda x: x + 'h')

    # Cantidad de Correos por hora
    lCorreosPorHora = dfEmail.groupby(dfEmail['Hora'])['Date'].count()
    lCorreosPorHora.plot(kind='bar', xlabel='Hora', ylabel='Cantidad de Correos', title=f'Cantidad de Correos por Hora - Total: {iTotalCorreos}')
    plt.show()



def fHistplotTopRemitentes(dfEmail, iTotalCorreos):
    # Top 10 de quien manda más correos
    lTopRemitentes = dfEmail['Mail'].value_counts().nlargest(20)
    lTopRemitentes.plot(kind='barh', xlabel='Cantidad de Correos', ylabel='Remitente', title=f'Top 20 de Remitentes que Envían más Correos - Total: {iTotalCorreos}')
    plt.gca().invert_yaxis()
    plt.show()

#########################################



################# MAIN ##################

fDatosBasicos()
fHistplotAnual(dfEmail, iTotalCorreos)
fHistplotMensual(dfEmail, iTotalCorreos)
fHistplotSemanal(dfEmail, iTotalCorreos)
fHistplotHoras(dfEmail, iTotalCorreos)
fHistplotTopRemitentes(dfEmail, iTotalCorreos)